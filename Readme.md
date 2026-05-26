# Vortex API Python Client

Official Python SDK for [Rupeezy Vortex APIs](https://vortex.rupeezy.in). Place orders, stream live quotes over WebSocket, run backtests, and look up any of ~190,000 NSE/BSE/MCX instruments by ticker.

## Installation

```bash
pip install vortex-api
```

## Quick start

```python
from vortex_api import VortexAPI
from vortex_api import Constants as Vc

client = VortexAPI("your_api_secret", "your_application_id")

# Log in via SSO
print(client.login_url(callback_param="hi"))
client.exchange_token("auth_code_received_in_callback_url")

# Place an order — preferred, ticker form
client.place_order(
    ticker="NSE:RELIANCE",
    transaction_type=Vc.TransactionSides.BUY,
    product=Vc.ProductTypes.DELIVERY,
    variety=Vc.VarietyTypes.REGULAR_LIMIT_ORDER,
    quantity=1,
    price=1700.0,
    trigger_price=0.0,
    disclosed_quantity=0,
    validity=Vc.ValidityTypes.FULL_DAY,
)

# Order book
client.orders(limit=20, offset=1)
```

## Identifying instruments

Most APIs accept either of:

- **`ticker`** (preferred) — a string like `"NSE:RELIANCE"` or `"NSE:NIFTY26JUL25000CE"`.
- **`exchange` + `token`** (legacy) — still works, but emits a `FutureWarning` urging migration to `ticker`.

This applies to `place_order`, `get_order_margin`, `historical_candles`, `subscribe`, and `unsubscribe`.

```python
# Preferred
client.place_order(ticker="NSE:RELIANCE", ...)

# Legacy (still works, deprecated)
client.place_order(exchange=Vc.ExchangeTypes.NSE_EQ, token=2885, ...)
```

## InstrumentManager

The SDK ships an `InstrumentManager` that mirrors the full Rupeezy instrument master (~190,000 NSE, BSE and MCX instruments) and gives you O(1) lookups by ticker, by `(exchange, token)`, or by ISIN. It downloads the master CSV once per IST trading day and serves every subsequent lookup from an in-memory index backed by a disk cache.

### Accessing it

```python
# Attached to every VortexAPI client
from vortex_api import VortexAPI
client = VortexAPI(...)
client.instruments.get_by_ticker("NSE:RELIANCE")

# Or standalone, no API client needed
from vortex_api import InstrumentManager
mgr = InstrumentManager()
mgr.get_by_ticker("NSE:RELIANCE")
```

The first lookup triggers a lazy load (~4 s on a cold cache, ~1 s from disk). Call `.load()` explicitly if you want to pay that cost up front instead of on the first lookup. `VortexFeed` does this for you before opening the websocket so that ticks arrive with `ticker` pre-resolved.

### Lookups

```python
# Forward — ticker → Instrument
reliance = client.instruments.get_by_ticker("NSE:RELIANCE")

# Reverse — (exchange, token) → Instrument
reliance = client.instruments.get_by_exchange_token("NSE_EQ", 2885)

# Convenience pair
exchange, token = client.instruments.ticker_to_token("NSE:RELIANCE")
ticker         = client.instruments.token_to_ticker("NSE_EQ", 2885)

# ISIN can map to multiple listings (NSE + BSE) — returns a list
listings = client.instruments.get_by_isin("INE002A01018")

# All F&O contracts under an underlying — option chain
nifty_options = client.instruments.all_by_underlying("NSE_FO", "NIFTY")

# Arbitrary predicate filter — anything you can express as a function
mtf_eligible = client.instruments.filter(lambda i: i.eligibility and i.series == "EQ")
```

Missing instruments raise `InstrumentNotFound`:

```python
from vortex_api import InstrumentNotFound

try:
    client.instruments.get_by_ticker("NSE:DOESNOTEXIST")
except InstrumentNotFound:
    ...
```

### The `Instrument` object

Each lookup returns an `Instrument` dataclass with the columns from the master file:

| Field | Type | Example |
|---|---|---|
| `ticker` | `str` | `"NSE:RELIANCE"` |
| `token` | `int` | `2885` |
| `exchange` | `str` | `"NSE_EQ"`, `"NSE_FO"`, `"BSE_EQ"`, `"MCX_FO"` |
| `symbol` | `str` | `"RELIANCE"` |
| `instrument_name` | `str` | `"EQUITY"` |
| `series` | `str` | `"EQ"`, `"BE"`, `"OPTIDX"`, `"FUTSTK"` |
| `expiry_date` | `str` | `"25JUL2026"` (F&O only) |
| `strike_price` | `float` | `25000.0` (options) |
| `option_type` | `str` | `"CE"` / `"PE"` (options) |
| `lot_size` | `int` | `250` |
| `tick` | `float` | `0.05` |
| `isin` | `str` | `"INE002A01018"` |
| `eligibility` | `bool` | `True` |
| `name` | `str` | Full security description |
| `last_trading_date` | `datetime` | F&O last trading day |
| `asm_gsm_stage` | `str` | ASM/GSM surveillance stage |

### Force refresh

The manager auto-refreshes whenever the disk cache is older than today's master publish. Trigger a manual refresh if you need to be sure you have the latest snapshot:

```python
client.instruments.refresh()   # forces an unconditional re-download
```

### How many are loaded

```python
client.instruments.count()        # → 188112
client.instruments.is_loaded      # → True once load() has run
```

## Streaming live quotes (WebSocket)

```python
from vortex_api import VortexFeed
from vortex_api import Constants as Vc
import time

def on_price_update(ws, ticks):
    for t in ticks:
        # Each tick carries `ticker`, `exchange`, `token`, prices...
        print(t["ticker"], t["last_trade_price"])

def on_order_update(ws, data):
    print(data)

def on_connect(ws, response):
    ws.subscribe(ticker="NSE:NIFTY",      mode=Vc.QuoteModes.LTP)
    ws.subscribe(ticker="NSE:BANKNIFTY",  mode=Vc.QuoteModes.OHLCV)
    ws.subscribe(ticker="NSE:RELIANCE",   mode=Vc.QuoteModes.FULL)

def main():
    wire = VortexFeed(access_token)   # auto-loads instruments before connect
    wire.on_price_update = on_price_update
    wire.on_order_update = on_order_update
    wire.on_connect = on_connect
    wire.connect(threaded=True)        # blocks briefly while instruments load,
                                       # then opens the websocket

    time.sleep(10)
    wire.unsubscribe(ticker="NSE:NIFTY")
    wire.unsubscribe(ticker="NSE:BANKNIFTY")
    wire.unsubscribe(ticker="NSE:RELIANCE")

if __name__ == "__main__":
    main()
```

### Tick payload

Every tick passed to `on_price_update` carries a `ticker` field client-side, resolved from `(exchange, token)` via the bundled `InstrumentManager`. The wire format still ships `exchange` + `token` — the SDK does the mapping locally so no server change is needed.

```python
{
    "ticker": "NSE:RELIANCE",        # added by SDK
    "exchange": "NSE_EQ",
    "token": 2885,
    "last_trade_price": 1734.55,
    # ... mode-specific fields
}
```

If the instrument master can't be loaded (network down, read-only filesystem), the feed continues working without the `ticker` field — `exchange` and `token` are always present.

## Backtesting

```python
from vortex_api import VortexAPI

client = VortexAPI(...)

# Save a backtest run for review on the developer portal.
# Accepts results from backtesting.py, vectorbt, or backtrader.
client.save_backtest_result(stats, name="SMA Crossover v2", symbol="NIFTY")
```

## Further reading

Full method reference: [vortex_api docs](https://vortex.rupeezy.in/docs/pyvortex/vortex_api.html)

API reference: [vortex.rupeezy.in/docs](https://vortex.rupeezy.in/docs)
