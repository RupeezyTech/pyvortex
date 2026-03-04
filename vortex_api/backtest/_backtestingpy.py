"""Serialization adapter for the backtesting.py library.

Handles stats objects from Backtest.run() and Backtest.optimize().
"""

from ._common import (
    _safe_float,
    _safe_float_zero,
    _duration_to_days,
    _date_str,
    compute_drawdown,
    compute_monthly_returns,
    build_payload,
)


def serialize_stats(stats, name, symbol, description, tags):
    """Serialize a backtesting.py stats object into the backend payload."""
    _sf = _safe_float_zero
    _dd = _duration_to_days

    summary = {
        "return_pct": _sf(stats.get("Return [%]")),
        "return_ann_pct": _sf(stats.get("Return (Ann.) [%]")),
        "volatility_ann_pct": _sf(stats.get("Volatility (Ann.) [%]")),
        "cagr_pct": _sf(stats.get("CAGR [%]")),
        "buy_hold_return_pct": _sf(stats.get("Buy & Hold Return [%]")),
        "alpha_pct": _sf(stats.get("Alpha [%]")),
        "beta": _sf(stats.get("Beta")),
        "sharpe_ratio": _sf(stats.get("Sharpe Ratio")),
        "sortino_ratio": _sf(stats.get("Sortino Ratio")),
        "calmar_ratio": _sf(stats.get("Calmar Ratio")),
        "max_drawdown_pct": _sf(stats.get("Max. Drawdown [%]")),
        "avg_drawdown_pct": _sf(stats.get("Avg. Drawdown [%]")),
        "max_drawdown_duration_days": _dd(stats.get("Max. Drawdown Duration")),
        "avg_drawdown_duration_days": _dd(stats.get("Avg. Drawdown Duration")),
        "equity_final": _sf(stats.get("Equity Final [$]")),
        "equity_peak": _sf(stats.get("Equity Peak [$]")),
        "commissions_total": _sf(stats.get("Commissions [$]")),
        "exposure_time_pct": _sf(stats.get("Exposure Time [%]")),
        "total_trades": int(stats.get("# Trades", 0)),
        "win_rate_pct": _sf(stats.get("Win Rate [%]")),
        "best_trade_pct": _sf(stats.get("Best Trade [%]")),
        "worst_trade_pct": _sf(stats.get("Worst Trade [%]")),
        "avg_trade_pct": _sf(stats.get("Avg. Trade [%]")),
        "max_trade_duration_days": _dd(stats.get("Max. Trade Duration")),
        "avg_trade_duration_days": _dd(stats.get("Avg. Trade Duration")),
        "profit_factor": _sf(stats.get("Profit Factor")),
        "expectancy_pct": _sf(stats.get("Expectancy [%]")),
        "sqn": _sf(stats.get("SQN")),
        "kelly_criterion": _sf(stats.get("Kelly Criterion")),
    }

    # --- Strategy name and parameters ---
    strategy_name = "Unknown"
    parameters = {}
    strategy = stats.get("_strategy")
    if strategy is not None:
        strategy_class = strategy if isinstance(strategy, type) else strategy.__class__
        strategy_name = strategy_class.__name__
        for attr in vars(strategy_class):
            if attr.startswith("_"):
                continue
            val = getattr(strategy_class, attr, None)
            if callable(val):
                continue
            if isinstance(val, (int, float, str, bool)):
                parameters[attr] = val

    # --- Equity curve ---
    equity_curve = []
    ec = stats.get("_equity_curve")
    if ec is not None and hasattr(ec, "index"):
        equity_series = ec["Equity"]
        step = max(1, len(equity_series) // 500)
        for i in range(0, len(equity_series), step):
            equity_curve.append({
                "date": equity_series.index[i].strftime("%Y-%m-%d"),
                "equity": round(float(equity_series.iloc[i]), 2),
            })
        if equity_curve and equity_curve[-1]["date"] != equity_series.index[-1].strftime("%Y-%m-%d"):
            equity_curve.append({
                "date": equity_series.index[-1].strftime("%Y-%m-%d"),
                "equity": round(float(equity_series.iloc[-1]), 2),
            })

    # --- Drawdown curve ---
    drawdown_curve = []
    if ec is not None and hasattr(ec, "index"):
        drawdown_curve = compute_drawdown(ec["Equity"])

    # --- Trade log ---
    trades_list = []
    trades = stats.get("_trades")
    if trades is not None and hasattr(trades, "iterrows"):
        for i, trade in trades.iterrows():
            size = trade.get("Size", 0)
            entry_time = trade.get("EntryTime")
            exit_time = trade.get("ExitTime")
            duration = 0
            if hasattr(entry_time, "strftime") and hasattr(exit_time, "strftime"):
                duration = (exit_time - entry_time).days
            trades_list.append({
                "trade_number": i + 1,
                "side": "LONG" if size > 0 else "SHORT",
                "size": abs(int(size)) if size else 0,
                "entry_bar": int(trade.get("EntryBar", 0)),
                "exit_bar": int(trade.get("ExitBar", 0)),
                "entry_date": _date_str(entry_time),
                "exit_date": _date_str(exit_time),
                "entry_price": _safe_float(trade.get("EntryPrice")),
                "exit_price": _safe_float(trade.get("ExitPrice")),
                "pnl_abs": _safe_float(trade.get("PnL")),
                "pnl_pct": _safe_float(trade.get("ReturnPct")),
                "duration_days": duration,
            })

    # --- Monthly returns ---
    monthly_returns = []
    if ec is not None and hasattr(ec, "index"):
        monthly_returns = compute_monthly_returns(ec["Equity"])

    # --- Dates ---
    start_date = _date_str(stats.get("Start"))
    end_date = _date_str(stats.get("End"))

    return build_payload(
        name=name,
        symbol=symbol,
        description=description,
        tags=tags,
        strategy_name=strategy_name,
        start_date=start_date,
        end_date=end_date,
        starting_capital=round(float(equity_curve[0]["equity"]), 2) if equity_curve else 0,
        parameters=parameters,
        summary=summary,
        equity_curve=equity_curve,
        drawdown_curve=drawdown_curve,
        trades=trades_list,
        monthly_returns=monthly_returns,
    )


# ─── Optimization ────────────────────────────────────────────────────────────

# Maps backtesting.py metric names to backend snake_case field names.
METRIC_NAME_MAP = {
    "Sharpe Ratio":           "sharpe_ratio",
    "Sortino Ratio":          "sortino_ratio",
    "Calmar Ratio":           "calmar_ratio",
    "Return [%]":             "return_pct",
    "Return (Ann.) [%]":      "return_ann_pct",
    "Equity Final [$]":       "equity_final",
    "SQN":                    "sqn",
    "Max. Drawdown [%]":      "max_drawdown_pct",
    "Avg. Drawdown [%]":      "avg_drawdown_pct",
    "Win Rate [%]":           "win_rate_pct",
    "Profit Factor":          "profit_factor",
    "Expectancy [%]":         "expectancy_pct",
    "# Trades":               "total_trades",
    "Exposure Time [%]":      "exposure_time_pct",
    "Buy & Hold Return [%]":  "buy_hold_return_pct",
    "CAGR [%]":               "cagr_pct",
    "Volatility (Ann.) [%]":  "volatility_ann_pct",
    "Kelly Criterion":        "kelly_criterion",
    "Best Trade [%]":         "best_trade_pct",
    "Worst Trade [%]":        "worst_trade_pct",
    "Avg. Trade [%]":         "avg_trade_pct",
}


def _infer_range_def(sorted_values):
    """Given a sorted list of unique float values, try to infer start/stop/step.

    If evenly spaced -> {"start", "stop", "step"}.
    Otherwise -> {"values": [...]}.
    """
    if len(sorted_values) < 2:
        return {"values": sorted_values}

    step = sorted_values[1] - sorted_values[0]
    is_uniform = all(
        abs((sorted_values[i] - sorted_values[i - 1]) - step) < 1e-9
        for i in range(2, len(sorted_values))
    )

    if is_uniform and step > 0:
        return {
            "start": sorted_values[0],
            "stop": sorted_values[-1] + step,
            "step": step,
        }
    return {"values": sorted_values}


def serialize_optimization(stats, heatmap, name, symbol, description,
                           objective_metric, maximize, param_ranges):
    """Serialize backtesting.py optimize() output into the backend payload."""
    # --- Map the objective metric name to backend format ---
    if isinstance(objective_metric, str):
        backend_metric = METRIC_NAME_MAP.get(objective_metric, objective_metric)
    else:
        # Callable (custom optimization function)
        backend_metric = "custom"

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
    elif heatmap is not None and hasattr(heatmap, "index"):
        index = heatmap.index
        if hasattr(index, "levels"):
            # MultiIndex (2+ params)
            for level_num, level_name in enumerate(index.names):
                values = sorted(set(float(v) for v in index.get_level_values(level_num)))
                parameter_defs[level_name] = _infer_range_def(values)
        else:
            # Plain Index (1 param)
            level_name = index.name or "param"
            values = sorted(set(float(v) for v in index))
            parameter_defs[level_name] = _infer_range_def(values)

    # --- Build results array from heatmap ---
    results = []
    if heatmap is not None and hasattr(heatmap, "index"):
        index = heatmap.index
        _METRIC_FIELDS = {"return_pct", "sharpe_ratio", "max_drawdown_pct", "total_trades"}

        def _build_entry(params, metric_val):
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
            return entry

        if hasattr(index, "levels"):
            for key, metric_val in heatmap.items():
                params = {}
                for i, level_name in enumerate(index.names):
                    params[level_name] = float(key[i])
                results.append(_build_entry(params, metric_val))
        else:
            level_name = index.name or "param"
            for key, metric_val in heatmap.items():
                results.append(_build_entry({level_name: float(key)}, metric_val))

    # --- Serialize the best result ---
    best_result = serialize_stats(stats, name, symbol, description, [])

    # --- Extract strategy name from stats ---
    strategy_name = "Unknown"
    strategy = stats.get("_strategy")
    if strategy is not None:
        strategy_class = strategy if isinstance(strategy, type) else strategy.__class__
        strategy_name = strategy_class.__name__

    return {
        "name": name[:200],
        "symbol": symbol[:50],
        "strategy_name": strategy_name,
        "description": description[:2000],
        "objective_metric": backend_metric,
        "maximize": maximize,
        "parameter_defs": parameter_defs,
        "results": results,
        "best_result": best_result,
    }
