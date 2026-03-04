"""Backtest serialization subpackage.

Auto-detects the backtesting library from the result object type and
dispatches to the appropriate adapter. Supports:

- backtesting.py (Backtest.run() / Backtest.optimize())
- vectorbt (vbt.Portfolio)
- backtrader (cerebro.run() Strategy)

Usage from api.py:
    from .backtest import serialize_stats, serialize_optimization
"""


def _is_backtestingpy(result):
    """Check if result is a backtesting.py stats object (pd.Series with _strategy key)."""
    return hasattr(result, "get") and result.get("_strategy") is not None


def _is_vectorbt(result):
    """Check if result is a vectorbt Portfolio object."""
    return (hasattr(result, "stats")
            and hasattr(result, "value")
            and hasattr(result, "trades"))


def _is_backtrader(result):
    """Check if result is a backtrader Strategy object."""
    # Single strategy
    if hasattr(result, "analyzers"):
        return True
    # List of strategies from cerebro.run()
    if isinstance(result, list) and len(result) > 0:
        item = result[0]
        if hasattr(item, "analyzers"):
            return True
        # Optimization result: list of lists
        if isinstance(item, list) and len(item) > 0 and hasattr(item[0], "analyzers"):
            return True
    return False


def serialize_stats(result, name, symbol, description, tags):
    """Auto-detect library and serialize backtest stats to backend payload.

    Args:
        result: Stats/Portfolio/Strategy object from any supported library.
        name: Backtest name.
        symbol: Trading symbol.
        description: Description text.
        tags: List of tag strings.

    Returns:
        dict: Payload ready for POST to /strategies/backtests.

    Raises:
        TypeError: If the result type is not recognized.
    """
    if _is_backtestingpy(result):
        from ._backtestingpy import serialize_stats as _serialize
        return _serialize(result, name, symbol, description, tags)

    if _is_vectorbt(result):
        from ._vectorbt import serialize_stats as _serialize
        return _serialize(result, name, symbol, description, tags)

    if _is_backtrader(result):
        from ._backtrader import serialize_stats as _serialize
        return _serialize(result, name, symbol, description, tags)

    raise TypeError(
        f"Unsupported backtest result type: {type(result).__name__}. "
        f"Expected stats from backtesting.py, a vectorbt Portfolio, "
        f"or a backtrader Strategy."
    )


def serialize_optimization(result, heatmap, name, symbol, description,
                           objective_metric, maximize, param_ranges):
    """Auto-detect library and serialize optimization results.

    Args:
        result: Stats/Portfolio/Strategy-list from any supported library.
        heatmap: Heatmap Series (backtesting.py/vectorbt) or metric_fn (backtrader).
        name: Optimization name.
        symbol: Trading symbol.
        description: Description text.
        objective_metric: Metric that was optimized.
        maximize: Boolean — True to maximize, False to minimize.
        param_ranges: Dict of param_name -> range/list.

    Returns:
        dict: Payload ready for POST to /strategies/optimizations.

    Raises:
        TypeError: If the result type is not recognized.
    """
    if _is_backtestingpy(result):
        from ._backtestingpy import serialize_optimization as _serialize
        return _serialize(result, heatmap, name, symbol, description,
                          objective_metric, maximize, param_ranges)

    if _is_vectorbt(result):
        from ._vectorbt import serialize_optimization as _serialize
        return _serialize(result, heatmap, name, symbol, description,
                          objective_metric, maximize, param_ranges)

    if _is_backtrader(result):
        from ._backtrader import serialize_optimization as _serialize
        return _serialize(result, heatmap, name, symbol, description,
                          objective_metric, maximize, param_ranges)

    raise TypeError(
        f"Unsupported optimization result type: {type(result).__name__}. "
        f"Expected stats from backtesting.py, a vectorbt Portfolio, "
        f"or backtrader optimization results."
    )
