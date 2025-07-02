"""
Vortex API client for Python -- [Visit Api Center](https://vortex.rupeezy.in).
Astha Credit & Securities Pvt. Ltd. (c) 2023

License
-------
Rupeezy's Vortex Python library is licensed under the MIT License

The library
-----------
Vortex APIs are meant for clients who want to execute orders based on their own strategy programatically and for partners to build their own applications. 
These apis provide a fast and secure way to place trades, manage positions and access real time market data.

The python client provides an abstraction over these APIs in order to seamlessly write applications and atrategies without 
the hassle of managing the apis. 

Getting started
---------------
    #!python
    from vortex_api import VortexAPI

    client = VortexAPI("your api secret","your application id")

    #For client login using SSO
    client.login_url(callback_param="hi)

    # Place order 

    client.place_order(client.EXCHANGE_NSE_EQUITY,22,client.TRANSACTION_TYPE_BUY,client.PRODUCT_DELIVERY,client.VARIETY_REGULAR_LIMIT_ORDER,1,1700,0,0,"DAY",1,True)

    #Get order book 
    client.orders(limit=20,offset=1)

"""
from __future__ import unicode_literals, absolute_import
from vortex_api.api import VortexAPI,Constants
from vortex_api.vortex_feed import VortexFeed
__all__ = [VortexAPI,Constants,VortexFeed]