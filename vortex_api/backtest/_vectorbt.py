"""Serialization adapter for vectorbt.

Handles vbt.Portfolio objects from vbt.Portfolio.from_signals() etc.
"""

from ._common import (
    _safe_float,
    _safe_float_zero,
    _date_str,
    _duration_to_days,
    compute_monthly_returns,
    compute_drawdown,
    compute_cagr,
    compute_avg_drawdown_stats,
    compute_sqn,
    compute_kelly,
    compute_exposure_time,
    build_payload,
)


# Maps vectorbt portfolio.stats() keys to backend field names.
_METRIC_MAP = {
    "Total Return [%]":     "return_pct",
    "Annualized Return [%]": "return_ann_pct",
    "Annualized Volatility [%]": "volatility_ann_pct",
    "Sharpe Ratio":         "sharpe_ratio",
    "Sortino Ratio":        "sortino_ratio",
    "Calmar Ratio":         "calmar_ratio",
    "Max Drawdown [%]":     "max_drawdown_pct",
    "Max Drawdown Duration": "max_drawdown_duration_days",
    "Total Closed Trades":  "total_trades",
    "Win Rate [%]":         "win_rate_pct",
    "Best Trade [%]":       "best_trade_pct",
    "Worst Trade [%]":      "worst_trade_pct",
    "Profit Factor":        "profit_factor",
    "Expectancy":           "expectancy_pct",
    "End Value":            "equity_final",
}


def _extract_summary(portfolio, st):
    """Build summary dict from vectorbt portfolio + stats Series.

    Produces all 30 fields matching backtesting.py output.
    """
    _sf = _safe_float_zero

    # --- Direct from stats() ---
    summary = {
        "return_pct": _sf(st.get("Total Return [%]")),
        "return_ann_pct": _sf(st.get("Annualized Return [%]")),
        "volatility_ann_pct": _sf(st.get("Annualized Volatility [%]")),
        "sharpe_ratio": _sf(st.get("Sharpe Ratio")),
        "sortino_ratio": _sf(st.get("Sortino Ratio")),
        "calmar_ratio": _sf(st.get("Calmar Ratio")),
        "max_drawdown_pct": _sf(st.get("Max Drawdown [%]")),
        "total_trades": int(st.get("Total Closed Trades", 0)),
        "win_rate_pct": _sf(st.get("Win Rate [%]")),
        "best_trade_pct": _sf(st.get("Best Trade [%]")),
        "worst_trade_pct": _sf(st.get("Worst Trade [%]")),
        "profit_factor": _sf(st.get("Profit Factor")),
        "expectancy_pct": _sf(st.get("Expectancy")),
        "equity_final": _sf(st.get("End Value")),
        "commissions_total": _sf(st.get("Total Fees Paid")),
    }

    # --- Max drawdown duration ---
    dd_dur = st.get("Max Drawdown Duration")
    if dd_dur is not None:
        summary["max_drawdown_duration_days"] = _duration_to_days(dd_dur)
    else:
        summary["max_drawdown_duration_days"] = 0

    # --- Benchmark return (if available) ---
    summary["buy_hold_return_pct"] = _sf(st.get("Benchmark Return [%]", 0))
    summary["alpha_pct"] = 0.0
    summary["beta"] = 0.0

    # --- Equity-derived metrics ---
    equity_series = portfolio.value()
    if equity_series is not None and len(equity_series) > 1:
        start_val = float(equity_series.iloc[0])
        end_val = float(equity_series.iloc[-1])
        n_days = (equity_series.index[-1] - equity_series.index[0]).days or 1

        # Equity peak
        summary["equity_peak"] = _sf(float(equity_series.max()))

        # CAGR
        summary["cagr_pct"] = compute_cagr(start_val, end_val, n_days)

        # Average drawdown stats
        avg_dd, avg_dd_dur = compute_avg_drawdown_stats(equity_series)
        summary["avg_drawdown_pct"] = avg_dd
        summary["avg_drawdown_duration_days"] = avg_dd_dur

        # Exposure time
        summary["exposure_time_pct"] = compute_exposure_time(equity_series)
    else:
        summary["equity_peak"] = summary["equity_final"]
        summary["cagr_pct"] = 0.0
        summary["avg_drawdown_pct"] = 0.0
        summary["avg_drawdown_duration_days"] = 0
        summary["exposure_time_pct"] = 0.0

    # --- Trade-derived metrics ---
    try:
        records = portfolio.trades.records_readable
        if records is not None and len(records) > 0:
            # avg_trade_pct — average of ALL trades, not just winning
            return_col = None
            for col_name in ("Return", "Return [%]", "PnL"):
                if col_name in records.columns:
                    return_col = col_name
                    break

            if return_col is not None:
                summary["avg_trade_pct"] = _sf(float(records[return_col].mean()))
            else:
                summary["avg_trade_pct"] = 0.0

            # Trade durations
            durations = []
            entry_col = None
            exit_col = None
            for ec in ("Entry Timestamp", "Entry Date"):
                if ec in records.columns:
                    entry_col = ec
                    break
            for xc in ("Exit Timestamp", "Exit Date"):
                if xc in records.columns:
                    exit_col = xc
                    break

            if entry_col and exit_col:
                for _, trade in records.iterrows():
                    et = trade.get(entry_col)
                    xt = trade.get(exit_col)
                    if hasattr(et, "strftime") and hasattr(xt, "strftime"):
                        durations.append((xt - et).days)

            if durations:
                summary["max_trade_duration_days"] = max(durations)
                summary["avg_trade_duration_days"] = int(round(sum(durations) / len(durations)))
            else:
                summary["max_trade_duration_days"] = 0
                summary["avg_trade_duration_days"] = 0

            # SQN
            pnl_col = "PnL" if "PnL" in records.columns else None
            if pnl_col:
                summary["sqn"] = compute_sqn(records[pnl_col].tolist())
            else:
                summary["sqn"] = 0.0

            # Kelly criterion
            win_rate_frac = summary["win_rate_pct"] / 100.0 if summary["win_rate_pct"] else 0
            winners = records[records.get("PnL", records.get("Return", 0)) > 0] if "PnL" in records.columns else None
            losers = records[records.get("PnL", records.get("Return", 0)) < 0] if "PnL" in records.columns else None

            if winners is not None and losers is not None and "PnL" in records.columns:
                w_mask = records["PnL"] > 0
                l_mask = records["PnL"] < 0
                avg_win = float(records.loc[w_mask, "PnL"].mean()) if w_mask.any() else 0
                avg_loss = float(records.loc[l_mask, "PnL"].abs().mean()) if l_mask.any() else 0
                summary["kelly_criterion"] = compute_kelly(win_rate_frac, avg_win, avg_loss)
            else:
                summary["kelly_criterion"] = 0.0
        else:
            summary["avg_trade_pct"] = 0.0
            summary["max_trade_duration_days"] = 0
            summary["avg_trade_duration_days"] = 0
            summary["sqn"] = 0.0
            summary["kelly_criterion"] = 0.0
    except Exception:
        summary["avg_trade_pct"] = 0.0
        summary["max_trade_duration_days"] = 0
        summary["avg_trade_duration_days"] = 0
        summary["sqn"] = 0.0
        summary["kelly_criterion"] = 0.0

    return summary


def serialize_stats(result, name, symbol, description, tags):
    """Serialize a vectorbt Portfolio into the backend payload.

    Args:
        result: A vbt.Portfolio object.
        name: Backtest name.
        symbol: Trading symbol.
        description: Description text.
        tags: List of tag strings.
    """
    st = result.stats()
    summary = _extract_summary(result, st)

    # --- Equity curve ---
    equity_series = result.value()
    equity_curve = []
    if equity_series is not None and len(equity_series) > 0:
        step = max(1, len(equity_series) // 500)
        for i in range(0, len(equity_series), step):
            equity_curve.append({
                "date": equity_series.index[i].strftime("%Y-%m-%d"),
                "equity": round(float(equity_series.iloc[i]), 2),
            })
        if equity_curve[-1]["date"] != equity_series.index[-1].strftime("%Y-%m-%d"):
            equity_curve.append({
                "date": equity_series.index[-1].strftime("%Y-%m-%d"),
                "equity": round(float(equity_series.iloc[-1]), 2),
            })

    # --- Drawdown curve ---
    drawdown_curve = []
    if equity_series is not None and len(equity_series) > 0:
        drawdown_curve = compute_drawdown(equity_series)

    # --- Trade log ---
    trades_list = []
    try:
        records = result.trades.records_readable
        for i, trade in records.iterrows():
            entry_time = trade.get("Entry Timestamp", trade.get("Entry Date"))
            exit_time = trade.get("Exit Timestamp", trade.get("Exit Date"))
            duration = 0
            if hasattr(entry_time, "strftime") and hasattr(exit_time, "strftime"):
                duration = (exit_time - entry_time).days

            size = trade.get("Size", 0)
            direction = trade.get("Direction", "Long")
            side = "SHORT" if str(direction).lower() == "short" else "LONG"

            trades_list.append({
                "trade_number": i + 1,
                "side": side,
                "size": abs(int(size)) if size else 0,
                "entry_bar": 0,
                "exit_bar": 0,
                "entry_date": _date_str(entry_time),
                "exit_date": _date_str(exit_time),
                "entry_price": _safe_float(trade.get("Avg Entry Price", trade.get("Entry Price"))),
                "exit_price": _safe_float(trade.get("Avg Exit Price", trade.get("Exit Price"))),
                "pnl_abs": _safe_float(trade.get("PnL")),
                "pnl_pct": _safe_float(trade.get("Return", trade.get("Return [%]"))),
                "duration_days": duration,
            })
    except Exception:
        pass

    # --- Monthly returns ---
    monthly_returns = []
    if equity_series is not None and len(equity_series) > 0:
        monthly_returns = compute_monthly_returns(equity_series)

    # --- Dates ---
    start_date = _date_str(st.get("Start"))
    end_date = _date_str(st.get("End"))

    return build_payload(
        name=name,
        symbol=symbol,
        description=description,
        tags=tags,
        strategy_name="VectorBT",
        start_date=start_date,
        end_date=end_date,
        starting_capital=round(float(equity_curve[0]["equity"]), 2) if equity_curve else 0,
        parameters={},
        summary=summary,
        equity_curve=equity_curve,
        drawdown_curve=drawdown_curve,
        trades=trades_list,
        monthly_returns=monthly_returns,
    )


def serialize_optimization(result, heatmap_or_metric, name, symbol,
                           description, objective_metric, maximize,
                           param_ranges):
    """Serialize a vectorbt multi-parameter Portfolio optimization.

    For vectorbt, the user typically passes:
      - result: the Portfolio from vbt.Portfolio.from_signals(..., param_product=True)
      - heatmap_or_metric: a pd.Series of metric values with (Multi)Index of param combos

    This can be produced via:
        returns = portfolio.total_return()  # Series indexed by param combos
        client.save_optimization_result(portfolio, returns, ...)
    """
    # --- Map objective metric ---
    backend_metric = _METRIC_MAP.get(objective_metric, objective_metric) \
        if isinstance(objective_metric, str) else "custom"

    # --- Build parameter_defs ---
    parameter_defs = {}
    if param_ranges is not None:
        for param_name, rng in param_ranges.items():
            if isinstance(rng, range):
                parameter_defs[param_name] = {
                    "start": float(rng.start),
                    "stop": float(rng.stop),
                    "step": float(rng.step),
                }
            elif hasattr(rng, "__iter__"):
                parameter_defs[param_name] = {
                    "values": [float(v) for v in rng],
                }
            else:
                parameter_defs[param_name] = {"values": [float(rng)]}
    elif heatmap_or_metric is not None and hasattr(heatmap_or_metric, "index"):
        index = heatmap_or_metric.index
        if hasattr(index, "levels"):
            for level_num, level_name in enumerate(index.names):
                values = sorted(set(float(v) for v in index.get_level_values(level_num)))
                from ._backtestingpy import _infer_range_def
                parameter_defs[str(level_name)] = _infer_range_def(values)
        else:
            level_name = str(index.name or "param")
            values = sorted(set(float(v) for v in index))
            from ._backtestingpy import _infer_range_def
            parameter_defs[level_name] = _infer_range_def(values)

    # --- Build results array ---
    results = []
    _METRIC_FIELDS = {"return_pct", "sharpe_ratio", "max_drawdown_pct", "total_trades"}

    if heatmap_or_metric is not None and hasattr(heatmap_or_metric, "items"):
        index = heatmap_or_metric.index
        if hasattr(index, "levels"):
            for key, metric_val in heatmap_or_metric.items():
                params = {}
                for i, level_name in enumerate(index.names):
                    params[str(level_name)] = float(key[i])
                val = _safe_float(metric_val)
                entry = {
                    "parameters": params,
                    "metric_value": val,
                    "return_pct": None,
                    "sharpe_ratio": None,
                    "max_drawdown_pct": None,
                    "total_trades": None,
                }
                if backend_metric in _METRIC_FIELDS and val is not None:
                    entry[backend_metric] = int(val) if backend_metric == "total_trades" else val
                results.append(entry)
        else:
            level_name = str(index.name or "param")
            for key, metric_val in heatmap_or_metric.items():
                val = _safe_float(metric_val)
                entry = {
                    "parameters": {level_name: float(key)},
                    "metric_value": val,
                    "return_pct": None,
                    "sharpe_ratio": None,
                    "max_drawdown_pct": None,
                    "total_trades": None,
                }
                if backend_metric in _METRIC_FIELDS and val is not None:
                    entry[backend_metric] = int(val) if backend_metric == "total_trades" else val
                results.append(entry)

    # --- Best result ---
    best_result = serialize_stats(result, name, symbol, description, [])

    return {
        "name": name[:200],
        "symbol": symbol[:50],
        "strategy_name": "VectorBT",
        "description": description[:2000],
        "objective_metric": backend_metric,
        "maximize": maximize,
        "parameter_defs": parameter_defs,
        "results": results,
        "best_result": best_result,
    }
