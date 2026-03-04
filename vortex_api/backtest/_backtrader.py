"""Serialization adapter for backtrader.

Handles Strategy objects from cerebro.run().
"""

from ._common import (
    _safe_float,
    _safe_float_zero,
    _date_str,
    _duration_to_days,
    compute_monthly_returns,
    compute_drawdown,
    compute_cagr,
    compute_volatility_ann,
    compute_sortino_ratio,
    compute_calmar_ratio,
    compute_avg_drawdown_stats,
    compute_sqn,
    compute_kelly,
    build_payload,
)


def _get_analyzer(strategy, name):
    """Safely retrieve a named analyzer from a backtrader strategy."""
    try:
        return getattr(strategy.analyzers, name)
    except AttributeError:
        return None


def _get_analysis(analyzer):
    """Safely call get_analysis() on an analyzer."""
    if analyzer is None:
        return {}
    try:
        return analyzer.get_analysis()
    except Exception:
        return {}


def _nested_get(d, *keys, default=None):
    """Safely traverse nested dicts/AutoOrderedDicts."""
    current = d
    for key in keys:
        if current is None:
            return default
        try:
            current = current[key]
        except (KeyError, TypeError, IndexError):
            try:
                current = getattr(current, key, None)
            except Exception:
                return default
        if current is None:
            return default
    return current


def _extract_summary(strategy, eq_series):
    """Build summary dict from backtrader strategy analyzers + equity curve.

    Produces all 30 fields matching backtesting.py output.

    Args:
        strategy: backtrader Strategy object.
        eq_series: pandas Series of equity values (datetime index), or None.
    """
    _sf = _safe_float_zero
    summary = {}

    # ── Analyzer-based metrics ────────────────────────────────────────

    # SharpeRatio
    sharpe = _get_analysis(_get_analyzer(strategy, "sharperatio"))
    summary["sharpe_ratio"] = _sf(sharpe.get("sharperatio")) if sharpe else 0.0

    # DrawDown
    dd = _get_analysis(_get_analyzer(strategy, "drawdown"))
    if dd:
        summary["max_drawdown_pct"] = _sf(_nested_get(dd, "max", "drawdown"))
        summary["max_drawdown_duration_days"] = int(_nested_get(dd, "max", "len", default=0))
    else:
        summary["max_drawdown_pct"] = 0.0
        summary["max_drawdown_duration_days"] = 0

    # Returns
    returns = _get_analysis(_get_analyzer(strategy, "returns"))
    if returns:
        rtot = returns.get("rtot")
        summary["return_pct"] = _sf(rtot * 100) if rtot is not None else 0.0
        summary["return_ann_pct"] = _sf(returns.get("rnorm100", 0))
    else:
        summary["return_pct"] = 0.0
        summary["return_ann_pct"] = 0.0

    # SQN (from analyzer if available)
    sqn_a = _get_analysis(_get_analyzer(strategy, "sqn"))
    summary["sqn"] = _sf(sqn_a.get("sqn")) if sqn_a else 0.0

    # TradeAnalyzer
    ta = _get_analysis(_get_analyzer(strategy, "tradeanalyzer"))
    total_closed = 0
    won_total = 0
    lost_total = 0
    won_pnl_total = 0.0
    lost_pnl_total = 0.0
    avg_won_pnl = 0.0
    avg_lost_pnl = 0.0

    if ta:
        total_closed = int(_nested_get(ta, "total", "closed", default=0) or 0)
        won_total = int(_nested_get(ta, "won", "total", default=0) or 0)
        lost_total = int(_nested_get(ta, "lost", "total", default=0) or 0)
        won_pnl_total = float(_nested_get(ta, "won", "pnl", "total", default=0) or 0)
        lost_pnl_total = float(_nested_get(ta, "lost", "pnl", "total", default=0) or 0)
        avg_won_pnl = float(_nested_get(ta, "won", "pnl", "average", default=0) or 0)
        avg_lost_pnl = float(_nested_get(ta, "lost", "pnl", "average", default=0) or 0)

    summary["total_trades"] = total_closed

    # Win rate
    if total_closed > 0:
        summary["win_rate_pct"] = _sf(won_total / total_closed * 100)
    else:
        summary["win_rate_pct"] = 0.0

    # Profit factor
    if lost_pnl_total and abs(lost_pnl_total) > 0:
        summary["profit_factor"] = _sf(won_pnl_total / abs(lost_pnl_total))
    else:
        summary["profit_factor"] = 0.0

    # Best/worst trade (absolute PnL from analyzer)
    summary["best_trade_pct"] = _sf(_nested_get(ta, "won", "pnl", "max")) if ta else 0.0
    summary["worst_trade_pct"] = _sf(_nested_get(ta, "lost", "pnl", "max")) if ta else 0.0

    # Trade durations
    if ta:
        avg_len = _nested_get(ta, "len", "average", default=0)
        max_len = _nested_get(ta, "len", "max", default=0)
        summary["avg_trade_duration_days"] = int(avg_len) if avg_len else 0
        summary["max_trade_duration_days"] = int(max_len) if max_len else 0
    else:
        summary["avg_trade_duration_days"] = 0
        summary["max_trade_duration_days"] = 0

    # Avg trade PnL (weighted average of winning + losing trades)
    if total_closed > 0:
        total_pnl = won_pnl_total + lost_pnl_total  # lost is negative
        summary["avg_trade_pct"] = _sf(total_pnl / total_closed)
    else:
        summary["avg_trade_pct"] = 0.0

    # Expectancy = avg_win * win_rate - avg_loss * loss_rate
    if total_closed > 0:
        win_rate_frac = won_total / total_closed
        loss_rate_frac = lost_total / total_closed
        expectancy = avg_won_pnl * win_rate_frac - abs(avg_lost_pnl) * loss_rate_frac
        summary["expectancy_pct"] = _sf(expectancy)
    else:
        summary["expectancy_pct"] = 0.0

    # Kelly criterion
    win_rate_frac = won_total / total_closed if total_closed > 0 else 0
    summary["kelly_criterion"] = compute_kelly(win_rate_frac, abs(avg_won_pnl), abs(avg_lost_pnl))

    # ── Equity-derived metrics ────────────────────────────────────────

    if eq_series is not None and len(eq_series) > 1:
        start_val = float(eq_series.iloc[0])
        end_val = float(eq_series.iloc[-1])
        n_days = (eq_series.index[-1] - eq_series.index[0]).days or 1
        daily_returns = eq_series.pct_change().dropna()

        summary["equity_final"] = _sf(end_val)
        summary["equity_peak"] = _sf(float(eq_series.max()))
        summary["cagr_pct"] = compute_cagr(start_val, end_val, n_days)
        summary["volatility_ann_pct"] = compute_volatility_ann(daily_returns)
        summary["sortino_ratio"] = compute_sortino_ratio(daily_returns)
        summary["calmar_ratio"] = compute_calmar_ratio(
            summary["cagr_pct"], summary["max_drawdown_pct"]
        )

        avg_dd, avg_dd_dur = compute_avg_drawdown_stats(eq_series)
        summary["avg_drawdown_pct"] = avg_dd
        summary["avg_drawdown_duration_days"] = avg_dd_dur
    else:
        summary["equity_final"] = 0.0
        summary["equity_peak"] = 0.0
        summary["cagr_pct"] = 0.0
        summary["volatility_ann_pct"] = 0.0
        summary["sortino_ratio"] = 0.0
        summary["calmar_ratio"] = 0.0
        summary["avg_drawdown_pct"] = 0.0
        summary["avg_drawdown_duration_days"] = 0

    # ── Not available without benchmark / position data ───────────────
    summary["buy_hold_return_pct"] = 0.0
    summary["alpha_pct"] = 0.0
    summary["beta"] = 0.0
    summary["commissions_total"] = 0.0
    summary["exposure_time_pct"] = 0.0

    return summary


def _extract_trades(strategy):
    """Extract trade list from TradeAnalyzer or trade notifications."""
    trades_list = []

    # Try using trade history if the strategy recorded it
    if hasattr(strategy, "_trades_log") and strategy._trades_log:
        for i, t in enumerate(strategy._trades_log):
            trades_list.append({
                "trade_number": i + 1,
                "side": t.get("side", "LONG"),
                "size": abs(int(t.get("size", 0))),
                "entry_bar": 0,
                "exit_bar": 0,
                "entry_date": _date_str(t.get("entry_date")),
                "exit_date": _date_str(t.get("exit_date")),
                "entry_price": _safe_float(t.get("entry_price")),
                "exit_price": _safe_float(t.get("exit_price")),
                "pnl_abs": _safe_float(t.get("pnl")),
                "pnl_pct": _safe_float(t.get("pnl_pct")),
                "duration_days": int(t.get("duration", 0)),
            })
        return trades_list

    # Fall back to TradeAnalyzer closed trades data
    ta = _get_analysis(_get_analyzer(strategy, "tradeanalyzer"))
    if not ta:
        return trades_list

    # TradeAnalyzer doesn't provide individual trade details in a list,
    # only aggregates. We return an empty list in that case.
    return trades_list


def _extract_equity_curve(strategy):
    """Extract equity curve from broker observer or custom attribute."""
    # Check for custom equity recording
    if hasattr(strategy, "_equity_curve") and strategy._equity_curve:
        curve = strategy._equity_curve
        equity_curve = []
        step = max(1, len(curve) // 500)
        for i in range(0, len(curve), step):
            entry = curve[i]
            equity_curve.append({
                "date": _date_str(entry.get("date", entry.get("datetime"))),
                "equity": round(float(entry.get("equity", entry.get("value", 0))), 2),
            })
        if equity_curve and len(curve) > 0:
            last = curve[-1]
            last_date = _date_str(last.get("date", last.get("datetime")))
            if equity_curve[-1]["date"] != last_date:
                equity_curve.append({
                    "date": last_date,
                    "equity": round(float(last.get("equity", last.get("value", 0))), 2),
                })
        return equity_curve

    # Try broker observer
    try:
        broker_obs = strategy.observers.broker
        if broker_obs is not None:
            values = broker_obs.lines.value.array
            dates = strategy.data.datetime.array
            from backtrader.utils.date import num2date
            equity_curve = []
            step = max(1, len(values) // 500)
            for i in range(0, len(values), step):
                if values[i] != 0:
                    dt = num2date(dates[i])
                    equity_curve.append({
                        "date": dt.strftime("%Y-%m-%d"),
                        "equity": round(float(values[i]), 2),
                    })
            if equity_curve and len(values) > 0:
                last_dt = num2date(dates[-1])
                last_date = last_dt.strftime("%Y-%m-%d")
                if equity_curve[-1]["date"] != last_date:
                    equity_curve.append({
                        "date": last_date,
                        "equity": round(float(values[-1]), 2),
                    })
            return equity_curve
    except Exception:
        pass

    return []


def serialize_stats(result, name, symbol, description, tags):
    """Serialize a backtrader Strategy into the backend payload.

    Args:
        result: A backtrader Strategy object (typically results[0] from
                cerebro.run()).
        name: Backtest name.
        symbol: Trading symbol.
        description: Description text.
        tags: List of tag strings.
    """
    # Handle list of strategies (cerebro.run() returns a list)
    strategy = result[0] if isinstance(result, list) else result

    # Extract equity curve FIRST so _extract_summary can use it
    equity_curve = _extract_equity_curve(strategy)
    trades_list = _extract_trades(strategy)

    # Build pandas equity Series for computations
    eq_series = None
    if equity_curve:
        try:
            import pandas as pd
            dates = [e["date"] for e in equity_curve]
            values = [e["equity"] for e in equity_curve]
            eq_series = pd.Series(values, index=pd.to_datetime(dates))
        except Exception:
            pass

    summary = _extract_summary(strategy, eq_series)

    # Drawdown and monthly returns from equity curve
    drawdown_curve = []
    monthly_returns = []
    if eq_series is not None and len(eq_series) > 1:
        try:
            drawdown_curve = compute_drawdown(eq_series)
            monthly_returns = compute_monthly_returns(eq_series)
        except Exception:
            pass

    # Dates
    start_date = equity_curve[0]["date"] if equity_curve else None
    end_date = equity_curve[-1]["date"] if equity_curve else None

    return build_payload(
        name=name,
        symbol=symbol,
        description=description,
        tags=tags,
        strategy_name=strategy.__class__.__name__ if hasattr(strategy, "__class__") else "Backtrader",
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


def serialize_optimization(results, metric_fn, name, symbol, description,
                           objective_metric, maximize, param_ranges):
    """Serialize backtrader optimization results.

    Args:
        results: List of strategy results from cerebro.run() after
                 cerebro.optstrategy(...).
        metric_fn: Callable that takes a strategy and returns the metric value,
                   or None to use return_pct.
        name: Optimization name.
        symbol: Trading symbol.
        description: Description text.
        objective_metric: Metric name string.
        maximize: Boolean.
        param_ranges: Dict of param_name -> range/list.
    """
    backend_metric = objective_metric if isinstance(objective_metric, str) else "custom"

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

    # --- Build results array ---
    opt_results = []
    best_strategy = None
    best_metric_val = None
    _METRIC_FIELDS = {"return_pct", "sharpe_ratio", "max_drawdown_pct", "total_trades"}

    for run in results:
        # Each run is a list with one strategy (or multiple if multiple datas)
        strat = run[0] if isinstance(run, list) else run

        # Extract parameters from the strategy
        params = {}
        if hasattr(strat, "params"):
            for pname in (param_ranges or {}):
                val = getattr(strat.params, pname, None)
                if val is not None:
                    params[pname] = float(val)

        # Compute metric value
        if metric_fn is not None:
            try:
                metric_val = metric_fn(strat)
            except Exception:
                metric_val = None
        else:
            # Default: use return_pct from Returns analyzer
            returns_a = _get_analysis(_get_analyzer(strat, "returns"))
            rtot = returns_a.get("rtot") if returns_a else None
            metric_val = rtot * 100 if rtot is not None else None

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
        opt_results.append(entry)

        # Track best
        if val is not None:
            if best_metric_val is None or (maximize and val > best_metric_val) or \
               (not maximize and val < best_metric_val):
                best_metric_val = val
                best_strategy = strat

    # --- Best result ---
    if best_strategy is not None:
        best_result = serialize_stats(best_strategy, name, symbol, description, [])
    else:
        best_result = serialize_stats(results[0] if results else [], name, symbol, description, [])

    # Strategy name
    strategy_name = "Backtrader"
    if best_strategy is not None and hasattr(best_strategy, "__class__"):
        strategy_name = best_strategy.__class__.__name__

    return {
        "name": name[:200],
        "symbol": symbol[:50],
        "strategy_name": strategy_name,
        "description": description[:2000],
        "objective_metric": backend_metric,
        "maximize": maximize,
        "parameter_defs": parameter_defs,
        "results": opt_results,
        "best_result": best_result,
    }
