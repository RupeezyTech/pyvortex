"""Shared utilities for backtest serialization across all supported libraries."""

import math


def _safe_float(val):
    """Convert a value to float, returning None for NaN/None/invalid."""
    if val is None:
        return None
    try:
        f = float(val)
        if math.isnan(f) or math.isinf(f):
            return None
        return round(f, 4)
    except (ValueError, TypeError):
        return None


def _safe_float_zero(val):
    """Like _safe_float but returns 0.0 instead of None for missing/invalid."""
    if val is None:
        return 0.0
    try:
        f = float(val)
        return 0.0 if (math.isnan(f) or math.isinf(f)) else round(f, 4)
    except (ValueError, TypeError):
        return 0.0


def _safe_isoformat(val):
    """Convert a datetime-like value to ISO format string."""
    if val is None:
        return None
    try:
        return val.isoformat()
    except AttributeError:
        return str(val)


def _duration_to_days(val):
    """Convert a timedelta-like value to integer days, returning 0 on failure."""
    if val is None:
        return 0
    try:
        return val.days
    except AttributeError:
        return 0


def _date_str(val):
    """Convert a datetime-like value to YYYY-MM-DD string."""
    if val is None:
        return None
    if hasattr(val, "strftime"):
        return val.strftime("%Y-%m-%d")
    return str(val)


def downsample_equity(dates, values, max_points=500):
    """Downsample parallel date/value arrays to at most max_points entries.

    Always includes the last point. Returns (dates, values) lists.
    """
    n = len(dates)
    if n <= max_points:
        return list(dates), list(values)

    step = max(1, n // max_points)
    sampled_dates = []
    sampled_values = []
    for i in range(0, n, step):
        sampled_dates.append(dates[i])
        sampled_values.append(values[i])

    # Ensure last point is included
    if sampled_dates[-1] != dates[-1]:
        sampled_dates.append(dates[-1])
        sampled_values.append(values[-1])

    return sampled_dates, sampled_values


def compute_drawdown(equity_series):
    """Compute drawdown % from a pandas equity Series.

    Returns list of dicts: {"date", "equity", "drawdown_pct"} — only rows
    where drawdown < -0.01%.
    """
    running_max = equity_series.cummax()
    drawdown = ((equity_series - running_max) / running_max) * 100

    step = max(1, len(drawdown) // 500)
    result = []
    for i in range(0, len(drawdown), step):
        dd_val = round(float(drawdown.iloc[i]), 4)
        if dd_val < -0.01:
            result.append({
                "date": drawdown.index[i].strftime("%Y-%m-%d"),
                "equity": round(float(equity_series.iloc[i]), 2),
                "drawdown_pct": dd_val,
            })
    return result


def compute_monthly_returns(equity_series):
    """Compute monthly % returns from a pandas equity Series.

    Returns list of dicts: {"year", "month", "return_pct"}.
    """
    try:
        monthly = equity_series.resample("ME").last()
        pct = monthly.pct_change().dropna() * 100
        result = []
        for date, ret in pct.items():
            result.append({
                "year": date.year,
                "month": date.month,
                "return_pct": _safe_float(ret),
            })
        return result
    except Exception:
        return []


def compute_cagr(start_value, end_value, n_days):
    """CAGR = (end/start)^(365/days) - 1, as percentage."""
    if not start_value or not end_value or n_days <= 0:
        return 0.0
    try:
        ratio = float(end_value) / float(start_value)
        if ratio <= 0:
            return 0.0
        cagr = (ratio ** (365.0 / n_days) - 1) * 100
        return _safe_float_zero(cagr)
    except (ValueError, ZeroDivisionError):
        return 0.0


def compute_volatility_ann(daily_returns, ann_factor=252):
    """Annualized volatility from daily returns Series, as percentage."""
    try:
        if daily_returns is None or len(daily_returns) < 2:
            return 0.0
        vol = float(daily_returns.std()) * (ann_factor ** 0.5) * 100
        return _safe_float_zero(vol)
    except Exception:
        return 0.0


def compute_sortino_ratio(daily_returns, risk_free=0, ann_factor=252):
    """Sortino = (mean_excess_return) / downside_deviation * sqrt(ann_factor)."""
    try:
        if daily_returns is None or len(daily_returns) < 2:
            return 0.0
        excess = daily_returns - risk_free / ann_factor
        downside = excess[excess < 0]
        if len(downside) == 0:
            return 0.0
        downside_std = float((downside ** 2).mean() ** 0.5)
        if downside_std == 0:
            return 0.0
        mean_excess = float(excess.mean())
        sortino = (mean_excess / downside_std) * (ann_factor ** 0.5)
        return _safe_float_zero(sortino)
    except Exception:
        return 0.0


def compute_calmar_ratio(cagr_pct, max_drawdown_pct):
    """Calmar = CAGR / abs(max_drawdown). Both inputs as percentages."""
    try:
        if not max_drawdown_pct or abs(float(max_drawdown_pct)) < 0.001:
            return 0.0
        return _safe_float_zero(float(cagr_pct) / abs(float(max_drawdown_pct)))
    except (ValueError, ZeroDivisionError):
        return 0.0


def compute_avg_drawdown_stats(equity_series):
    """Compute average drawdown % and average drawdown duration in days.

    Returns (avg_drawdown_pct, avg_drawdown_duration_days).
    """
    try:
        if equity_series is None or len(equity_series) < 2:
            return 0.0, 0
        running_max = equity_series.cummax()
        dd_pct = ((equity_series - running_max) / running_max) * 100

        # Identify drawdown periods
        in_dd = dd_pct < -0.01
        dd_depths = []
        dd_lengths = []
        current_depth = 0.0
        current_len = 0

        for val, is_dd in zip(dd_pct, in_dd):
            if is_dd:
                fval = float(val)
                current_depth = min(current_depth, fval)
                current_len += 1
            else:
                if current_len > 0:
                    dd_depths.append(current_depth)
                    dd_lengths.append(current_len)
                    current_depth = 0.0
                    current_len = 0
        # Close last drawdown if still open
        if current_len > 0:
            dd_depths.append(current_depth)
            dd_lengths.append(current_len)

        if not dd_depths:
            return 0.0, 0

        avg_dd = sum(dd_depths) / len(dd_depths)
        avg_len = sum(dd_lengths) / len(dd_lengths)
        return _safe_float_zero(avg_dd), int(round(avg_len))
    except Exception:
        return 0.0, 0


def compute_sqn(trade_pnls):
    """SQN (System Quality Number) = sqrt(n) * mean(pnl) / std(pnl)."""
    try:
        if trade_pnls is None or len(trade_pnls) < 2:
            return 0.0
        pnls = [float(p) for p in trade_pnls if p is not None]
        n = len(pnls)
        if n < 2:
            return 0.0
        mean = sum(pnls) / n
        variance = sum((x - mean) ** 2 for x in pnls) / n
        std = variance ** 0.5
        if std == 0:
            return 0.0
        return _safe_float_zero(mean / std * (n ** 0.5))
    except Exception:
        return 0.0


def compute_kelly(win_rate_frac, avg_win, avg_loss):
    """Kelly criterion = W - (1-W) / R, where R = avg_win/avg_loss.

    win_rate_frac: 0-1 fraction, avg_win/avg_loss: absolute values.
    """
    try:
        if not avg_loss or float(avg_loss) == 0 or not avg_win:
            return 0.0
        w = float(win_rate_frac)
        r = abs(float(avg_win)) / abs(float(avg_loss))
        if r == 0:
            return 0.0
        return _safe_float_zero(w - (1 - w) / r)
    except (ValueError, ZeroDivisionError):
        return 0.0


def compute_exposure_time(equity_series):
    """Estimate exposure time as % of bars where equity value changed.

    This is a proxy — in a position, equity changes with price; out of
    position, equity stays flat.
    """
    try:
        if equity_series is None or len(equity_series) < 2:
            return 0.0
        changes = equity_series.diff().iloc[1:]
        non_zero = (changes.abs() > 0.001).sum()
        return _safe_float_zero(float(non_zero) / len(changes) * 100)
    except Exception:
        return 0.0


def build_payload(*, name, symbol, description, tags, strategy_name,
                  start_date, end_date, starting_capital, parameters,
                  summary, equity_curve, drawdown_curve, trades,
                  monthly_returns):
    """Assemble the final payload dict for the backend POST."""
    return {
        "name": name[:200],
        "symbol": symbol[:50],
        "description": description[:2000],
        "tags": (tags or [])[:20],
        "strategy_name": strategy_name,
        "start_date": start_date,
        "end_date": end_date,
        "starting_capital": starting_capital,
        "parameters": parameters or {},
        "summary": summary,
        "equity_curve": equity_curve,
        "drawdown_curve": drawdown_curve,
        "trades": trades,
        "monthly_returns": monthly_returns,
    }
