"""
Vortex API client for Python -- [Visit Api Center](https://vortex.rupeezy.in).
Astha Credit & Securities Pvt. Ltd. (c) 2023

License
-------
Rupeezy's Vortex Python library is licensed under the MIT License

The library
-----------
Vortex APIs are meant for clients who want to execute orders based on their own
strategy programmatically and for partners to build their own applications.
These APIs provide a fast and secure way to place trades, manage positions, and
access real-time market data.

The Python client provides an abstraction over these APIs so you can write
applications and strategies without dealing with HTTP, websocket framing, or
instrument-master plumbing yourself.

Getting started
---------------
    #!python
    from vortex_api import VortexAPI
    from vortex_api import Constants as Vc

    client = VortexAPI("your_api_secret", "your_application_id")

    # SSO login
    print(client.login_url(callback_param="hi"))
    client.exchange_token("auth_token_received_in_callback_url")

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

InstrumentManager
-----------------
``client.instruments`` is an :class:`InstrumentManager` that mirrors the full
Rupeezy instrument master (~190,000 NSE/BSE/MCX rows) and gives you O(1)
lookups by ticker, ``(exchange, token)``, or ISIN. The first lookup triggers
a lazy load; subsequent lookups are in-memory hits. The master is cached on
disk so the SDK downloads it at most once per IST trading day.

    #!python
    inst = client.instruments.get_by_ticker("NSE:RELIANCE")
    print(inst.token, inst.lot_size, inst.isin)

    # Reverse lookup
    inst = client.instruments.get_by_exchange_token("NSE_EQ", 2885)

    # ISIN → list of listings (one ISIN can list on multiple exchanges)
    listings = client.instruments.get_by_isin("INE002A01018")

    # F&O chain — all contracts under an underlying
    chain = client.instruments.all_by_underlying("NSE_FO", "NIFTY")

    # Predicate filter
    eq = client.instruments.filter(lambda i: i.exchange == "NSE_EQ" and i.eligibility)

    # Force a refresh (otherwise auto-refreshed daily)
    client.instruments.refresh()

You can also use the manager standalone, without a :class:`VortexAPI` client::

    #!python
    from vortex_api import InstrumentManager
    mgr = InstrumentManager()
    mgr.get_by_ticker("NSE:RELIANCE")

Streaming quotes
----------------
    #!python
    from vortex_api import VortexFeed
    from vortex_api import Constants as Vc

    def on_price_update(ws, ticks):
        for t in ticks:
            # Each tick carries `ticker`, `exchange`, `token`, prices...
            print(t["ticker"], t["last_trade_price"])

    def on_connect(ws, response):
        ws.subscribe(ticker="NSE:RELIANCE", mode=Vc.QuoteModes.FULL)

    wire = VortexFeed(access_token)
    wire.on_price_update = on_price_update
    wire.on_connect = on_connect
    wire.connect(threaded=True)
"""
from __future__ import unicode_literals, absolute_import
from vortex_api.api import VortexAPI,Constants
from vortex_api.vortex_feed import VortexFeed
from vortex_api.instruments import InstrumentManager, Instrument, InstrumentNotFound
__all__ = [VortexAPI,Constants,VortexFeed,InstrumentManager,Instrument,InstrumentNotFound]