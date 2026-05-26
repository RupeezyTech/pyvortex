import six
import sys
import time
import json
import struct
import logging
import threading
import os
import warnings
from twisted.internet import reactor, ssl
from twisted.python import log as twisted_log
from twisted.internet.protocol import ReconnectingClientFactory
from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory, connectWS

from .__version__ import __version__, __name__

log = logging.getLogger(__name__)


def _warn_token_exchange_deprecated(method_name: str) -> None:
    """Emit a FutureWarning when callers pass ``token``/``exchange`` instead of ``ticker``."""
    warnings.warn(
        f"Passing 'exchange' and 'token' to {method_name}() is deprecated and will be "
        "removed in a future release. Use 'ticker' instead (e.g. ticker=\"NSE:RELIANCE\").",
        FutureWarning,
        stacklevel=3,
    )

class ClientProtocol(WebSocketClientProtocol): 
    """
    A WebSocket client protocol that implements ping-pong keepalive.

    Args:
        PING_INTERVAL: The interval in seconds between sending pings.
        KEEPALIVE_INTERVAL: The interval in seconds after which a connection is considered dead if nothing (no pong, no message) has been received.
    """
    PING_INTERVAL = 2.5
    # Generous on purpose: a strategy busy in its tick callback reads the pong late,
    # and a 5 s limit dropped healthy sockets every ~7 s in a reconnect loop.
    KEEPALIVE_INTERVAL = 30

    _ping_message = ""
    _next_ping = None
    _next_pong_check = None
    _last_pong_time = None
    _last_ping_time = None

    def __init__(self, *args, **kwargs):
        super(ClientProtocol, self).__init__(*args, **kwargs)

    def onConnect(self, response):  
        """
        Called when the connection is established.

        Args:
            response: The response from the server.
        """
        self.factory.ws = self

        if self.factory.on_connect:
            self.factory.on_connect(self, response)

        # Reset reconnect on successful reconnect
        self.factory.resetDelay()

    def onOpen(self):  
        """
        Called when the connection is opened.

        Sends a ping and starts a timer to check for pongs.
        """
        # send ping
        self._loop_ping()
        # Start a timer to check for pongs
        self._loop_pong_check()

        if self.factory.on_open:
            self.factory.on_open(self)

    def onMessage(self, payload, is_binary):  
        """
        Called when a message is received.

        Args:
            payload: The message payload.
            is_binary: Whether the message is binary.
        """
        # Any frame from the server proves the connection is alive.
        self._last_pong_time = time.time()
        if self.factory.on_message:
            self.factory.on_message(self, payload, is_binary)

    def onClose(self, was_clean, code, reason):  
        """
        Called when the connection is closed.

        Args:
            was_clean: Whether the connection was closed cleanly.
            code: The close code.
            reason: The close reason.
        """
        if not was_clean:
            if self.factory.on_error:
                self.factory.on_error(self, code, reason)

        if self.factory.on_close:
            self.factory.on_close(self, code, reason)

        # Cancel next ping and timer
        self._last_ping_time = None
        self._last_pong_time = None

        if self._next_ping:
            self._next_ping.cancel()

        if self._next_pong_check:
            self._next_pong_check.cancel()

    def onPong(self, response):  
        """
        Called when a pong message is received.

        Args:
            response: The pong message.
        """
        if self._last_pong_time and self.factory.debug:
            log.debug("last pong was {} seconds back.".format(time.time() - self._last_pong_time))

        self._last_pong_time = time.time()

        if self.factory.debug:
            log.debug("pong => {}".format(response))

    def _loop_ping(self):  
        """
        Sends a ping message every X seconds.
        """
        if self.factory.debug:
            log.debug("ping => {}".format(self._ping_message))
            if self._last_ping_time:
                log.debug("last ping was {} seconds back.".format(time.time() - self._last_ping_time))

        # Set current time as last ping time
        self._last_ping_time = time.time()
        # Send a ping message to server
        self.sendPing(self._ping_message)

        # Call self after X seconds
        self._next_ping = self.factory.reactor.callLater(self.PING_INTERVAL, self._loop_ping)

    def _loop_pong_check(self):
        """
        Checks if the connection is still alive by checking the last pong time.
        If no pong has been received in X seconds, the connection is considered dead and is dropped.
        """
        if self._last_pong_time:
            # No pong message since long time, so init reconnect
            last_pong_diff = time.time() - self._last_pong_time
            if last_pong_diff > self.KEEPALIVE_INTERVAL:
                if self.factory.debug:
                    log.debug("Last pong was {} seconds ago. So dropping connection to reconnect.".format(
                        last_pong_diff))
                # drop existing connection to avoid ghost connection
                self.dropConnection(abort=True)

        # Call self after X seconds
        self._next_pong_check = self.factory.reactor.callLater(self.PING_INTERVAL, self._loop_pong_check)

class ClientFactory(WebSocketClientFactory,ReconnectingClientFactory): 
    """
    A WebSocket client factory that implements reconnect logic.

    Args:
        protocol: The WebSocket protocol to use.
        maxDelay: The maximum delay in seconds between retries.
        maxRetries: The maximum number of retries.
    """
    protocol = ClientProtocol
    maxDelay = 5
    maxRetries = 10

    _last_connection_time = None

    def __init__(self, *args, **kwargs):
        """Initialize with default callback method values."""
        self.ws = None
        self.on_open = None
        self.on_error = None
        self.on_close = None
        self.on_message = None
        self.on_connect = None
        self.on_reconnect = None
        self.on_noreconnect = None

        super(ClientFactory, self).__init__(*args, **kwargs)

    def startedConnecting(self, connector):  
        """Called when the connection is started or reconnected."""
        if not self._last_connection_time and self.debug:
            log.debug("Start WebSocket connection.")

        self._last_connection_time = time.time()

    def clientConnectionFailed(self, connector, reason):  
        """Called when the connection fails."""
        if self.retries > 0:
            log.error("Retrying connection. Retry attempt count: {}. Next retry in around: {} seconds".format(self.retries, int(round(self.delay))))

            # on reconnect callback
            if self.on_reconnect:
                self.on_reconnect(self.retries)

        # Retry the connection
        self.retry(connector)
        self.send_noreconnect()

    def clientConnectionLost(self, connector, reason):  
        """Called when the connection is lost."""
        if self.retries > 0:
            # on reconnect callback
            if self.on_reconnect:
                self.on_reconnect(self.retries)

        # Retry the connection
        self.retry(connector)
        self.send_noreconnect()

    def send_noreconnect(self):
        """Called when the maximum number of retries has been exhausted."""
        if self.maxRetries is not None and (self.retries > self.maxRetries):
            if self.debug:
                log.debug("Maximum retries ({}) exhausted.".format(self.maxRetries))
                # Stop the loop for exceeding max retry attempts
                self.stop()

            if self.on_noreconnect:
                self.on_noreconnect()

class VortexFeed: 
    """
    The WebSocket client for connecting to vortex's live price and order streaming service
    """
    CONNECT_TIMEOUT = 30
    # Default Reconnect max delay.
    RECONNECT_MAX_DELAY = 60
    # Default reconnect attempts
    RECONNECT_MAX_TRIES = 50
    _is_first_connect = True
    _message_subscribe = "subscribe"
    _message_unsubscribe = "unsubscribe"

    def __init__(self, access_token: str=None, websocket_endpoint="wss://wire.rupeezy.in/ws",reconnect=True, reconnect_max_tries=RECONNECT_MAX_TRIES, reconnect_max_delay=RECONNECT_MAX_DELAY,
                 connect_timeout=CONNECT_TIMEOUT, debug = False, instruments=None) -> None:
        """
        Args:
            instruments: Optional ``InstrumentManager``. If you don't pass one,
                ``VortexFeed`` creates its own automatically — every tick
                parsed from the binary feed will be tagged with a ``ticker``
                field (e.g. ``"NSE:RELIANCE"``) alongside the existing
                ``exchange``/``token`` fields. Pass an existing manager (e.g.
                ``client.instruments``) if you want to share one cache across
                the REST client and the feed.

                ``connect()`` blocks until the instrument master is loaded so
                every tick — including the very first one — is guaranteed to
                carry a ``ticker`` field. The load is cached on disk and only
                hits the network once per IST trading day, so this overhead is
                near-zero on every connect after the first of the day.
        """
        self._maximum_reconnect_max_tries = self.RECONNECT_MAX_TRIES
        self._minimum_reconnect_max_delay = 0 
        if reconnect == False: 
            self.reconnect_max_tries = 0 
        elif reconnect_max_tries > self._maximum_reconnect_max_tries:
            log.warning("`reconnect_max_tries` can not be more than {val}. Setting to highest possible value - {val}.".format(
                val=self._maximum_reconnect_max_tries))
            self.reconnect_max_tries = self._maximum_reconnect_max_tries
        else:
            self.reconnect_max_tries = reconnect_max_tries
        
        if reconnect_max_delay < self._minimum_reconnect_max_delay:
            log.warning("`reconnect_max_delay` can not be less than {val}. Setting to lowest possible value - {val}.".format(
                val=self._minimum_reconnect_max_delay))
            self.reconnect_max_delay = self._minimum_reconnect_max_delay
        else:
            self.reconnect_max_delay = reconnect_max_delay
        
        self.connect_timeout = connect_timeout
        if access_token == None:
            if os.getenv("VORTEX_ACCESS_TOKEN") != None:
                access_token = os.getenv("VORTEX_ACCESS_TOKEN")
            else:
                raise ValueError("Access token must be provided either as an argument or through the VORTEX_ACCESS_TOKEN environment variable.")
        
        if os.getenv("VORTEX_FEED_BASE_URL") != None:
            self.socket_url = os.getenv("VORTEX_FEED_BASE_URL") + "?auth_token=" + access_token
        else:
            self.socket_url = websocket_endpoint+"?auth_token="+access_token

        self.access_token = access_token
        self.socket_token = self.__getSocketToken__(self.access_token)

        self.debug = debug
        # self.on_price_update = None
        self.on_price_update = None
        self.on_open = None
        self.on_close = None
        self.on_error = None
        self.on_connect = None
        self.on_message = None
        self.on_reconnect = None
        self.on_noreconnect = None
        self.on_order_update = None
        if instruments is None:
            # Default: every feed gets its own InstrumentManager so ticker
            # enrichment "just works" without any wiring from the user. The
            # underlying disk cache is process-wide, so multiple managers in
            # the same process share the same master.csv on disk.
            try:
                from .instruments import InstrumentManager
                instruments = InstrumentManager()
            except Exception as exc:
                # Never let an instruments-import problem break the feed.
                log.debug("Could not auto-construct InstrumentManager: %s", exc)
                instruments = None
        self.instruments = instruments
        # Flat dict of active subscriptions, keyed by the canonical identifier
        # (the ``ticker`` string, or ``"<exchange>:<token>"`` for the legacy form).
        # Each value is a dict carrying the original args plus the mode so that
        # ``resubscribe()`` can replay each subscription exactly as the user
        # originally requested it.
        self.subscribed_tokens = {}
        pass

    def __getSocketToken__(self,access_token: str)->str:
        return 
    
    def _create_connection(self, url, **kwargs):
        self.factory = ClientFactory(url,self.debug, **kwargs)
        self.ws = self.factory.ws
        self.factory.debug = self.debug

        self.factory.on_open = self._on_open
        self.factory.on_error = self._on_error
        self.factory.on_close = self._on_close
        self.factory.on_message = self._on_message
        self.factory.on_connect = self._on_connect
        self.factory.on_reconnect = self._on_reconnect
        self.factory.on_noreconnect = self._on_noreconnect

        self.factory.maxDelay = self.reconnect_max_delay
        self.factory.maxRetries = self.reconnect_max_tries
    
    def _user_agent(self):
        return (__name__ + "-python/").capitalize() + __version__
    
    def connect(self, threaded=False, disable_ssl_verification=False):
        """
        Establish a websocket connection.
        - `disable_ssl_verification` disables building ssl context
        """
        # Load the instrument master *before* opening the websocket so the
        # very first tick is already eligible for ticker enrichment. The
        # InstrumentManager caches on disk (1× per IST day across processes),
        # so this is near-instant on every connect after the day's first.
        self._preload_instruments()

        # Init WebSocket client factory
        self._create_connection(self.socket_url,
                                useragent=self._user_agent())

        # Set SSL context
        context_factory = None
        if self.factory.isSecure and not disable_ssl_verification:
            from urllib.parse import urlparse
            hostname = urlparse(self.socket_url).hostname
            context_factory = ssl.optionsForClientTLS(hostname)

        # Establish WebSocket connection to a server
        connectWS(self.factory, contextFactory=context_factory, timeout=self.connect_timeout)

        if self.debug:
            twisted_log.startLogging(sys.stdout)

        # Run in seperate thread of blocking
        opts = {}
        # Run when reactor is not running
        if not reactor.running:
            if threaded:
                # Signals are not allowed in non main thread by twisted so suppress it.
                opts["installSignalHandlers"] = False
                self.websocket_thread = threading.Thread(target=reactor.run, kwargs=opts)
                self.websocket_thread.daemon = True
                self.websocket_thread.start()
            else:
                reactor.run(**opts)
        else: 
            print(reactor.running)

    def is_connected(self):
        """Check if WebSocket connection is established."""
        if self.ws and self.ws.state == self.ws.STATE_OPEN:
            return True
        else:
            return False

    def _close(self, code=None, reason=None):
        """Close the WebSocket connection."""
        if self.ws:
            self.ws.sendClose(code, reason)

    def close(self, code=None, reason=None):
        """Close the WebSocket connection."""
        self.stop_retry()
        self._close(code, reason)

    def stop(self):
        """Stop the event loop. Should be used if main thread has to be closed in `on_close` method.
        Reconnection mechanism cannot happen past this method
        """
        reactor.stop()

    def stop_retry(self):
        """Stop auto retry when it is in progress."""
        if self.factory:
            self.factory.stopTrying()

    def subscribe(self, exchange: str = None, token: int = None, mode: str = None, ticker: str = None) -> bool:
        """
        Subscribe to live updates for an instrument.

        Pass either ``ticker`` (e.g. ``"NSE:RELIANCE"``) or both ``exchange`` and
        ``token``. The ``(exchange, token)`` form is supported for backward
        compatibility but is deprecated — prefer ``ticker``.

        Args:
            exchange (str, optional): Exchange code (e.g. ``"NSE_EQ"``).
                Required together with ``token`` when ``ticker`` is not supplied.
            token (int, optional): Security token. Required together with
                ``exchange`` when ``ticker`` is not supplied.
            mode (str): Quote mode — ``"ltp"``, ``"ohlcv"``, or ``"full"``.
            ticker (str, optional): Instrument ticker in the form
                ``<EXCHANGE>:<SYMBOL>`` (e.g. ``"NSE:RELIANCE"``).

        Raises:
            ValueError: If neither ``ticker`` nor (``exchange`` and ``token``) is provided.
            TypeError: If ``mode`` is missing.
        """
        if ticker is None and (exchange is None or token is None):
            raise ValueError("Either 'ticker' or both 'exchange' and 'token' must be provided")
        if mode is None:
            raise TypeError("missing required argument: 'mode'")

        if ticker is None:
            _warn_token_exchange_deprecated("subscribe")

        payload = {"message_type": self._message_subscribe, "mode": mode}
        if ticker is not None:
            payload["ticker"] = ticker
            sub_key = ticker
            record = {"ticker": ticker, "mode": mode}
        else:
            payload["exchange"] = exchange
            payload["token"] = token
            sub_key = f"{exchange}:{token}"
            record = {"exchange": exchange, "token": token, "mode": mode}

        try:
            self.ws.sendMessage(six.b(json.dumps(payload)))
            self.subscribed_tokens[sub_key] = record
            return True
        except Exception as e:
            self._close(reason="Error while subscribe: {}".format(str(e)))
            raise

    def unsubscribe(self, exchange: str = None, token: int = None, ticker: str = None) -> bool:
        """
        Unsubscribe from live updates for an instrument.

        Pass either ``ticker`` or both ``exchange`` and ``token`` — must match
        the form originally used in ``subscribe()`` for the local subscription
        record to be cleared (the server unsubscribes either way).

        Args:
            exchange (str, optional): Exchange code. Required together with
                ``token`` when ``ticker`` is not supplied.
            token (int, optional): Security token. Required together with
                ``exchange`` when ``ticker`` is not supplied.
            ticker (str, optional): Instrument ticker in the form
                ``<EXCHANGE>:<SYMBOL>`` (e.g. ``"NSE:RELIANCE"``).

        Raises:
            ValueError: If neither ``ticker`` nor (``exchange`` and ``token``) is provided.
        """
        if ticker is None and (exchange is None or token is None):
            raise ValueError("Either 'ticker' or both 'exchange' and 'token' must be provided")

        if ticker is None:
            _warn_token_exchange_deprecated("unsubscribe")

        payload = {"message_type": self._message_unsubscribe}
        if ticker is not None:
            payload["ticker"] = ticker
            sub_key = ticker
        else:
            payload["exchange"] = exchange
            payload["token"] = token
            sub_key = f"{exchange}:{token}"

        try:
            self.ws.sendMessage(six.b(json.dumps(payload)))
            self.subscribed_tokens.pop(sub_key, None)
            return True
        except Exception as e:
            self._close(reason="Error while unsubscribe: {}".format(str(e)))
            raise

    def resubscribe(self):
        """Resubscribe to all currently-tracked instruments.

        Each subscription is replayed in the exact form (ticker or
        exchange/token) the user originally used.
        """
        # Snapshot first because subscribe() mutates the dict.
        for record in list(self.subscribed_tokens.values()):
            if "ticker" in record:
                self.subscribe(ticker=record["ticker"], mode=record["mode"])
            else:
                self.subscribe(
                    exchange=record["exchange"],
                    token=record["token"],
                    mode=record["mode"],
                )

    def _on_connect(self, ws, response):
        self.ws = ws
        if self.on_connect:
            self.on_connect(self, response)

    def _on_close(self, ws, code, reason):
        """Call `on_close` callback when connection is closed."""
        log.error("Connection closed: {} - {}".format(code, str(reason)))

        if self.on_close:
            self.on_close(self, code, reason)

    def _on_error(self, ws, code, reason):
        """Call `on_error` callback when connection throws an error."""
        log.error("Connection error: {} - {}".format(code, str(reason)))

        if self.on_error:
            self.on_error(self, code, reason)

    def _on_message(self, ws, payload, is_binary):
        """Call `on_message` callback when text message is received."""
        if self.on_message:
            self.on_message(self, payload, is_binary)

        # If the message is binary, parse it and send it to the callback.
        if self.on_price_update and is_binary and len(payload) > 4:
            self.on_price_update(self, self._parse_binary(payload))

        # Parse text messages
        if not is_binary:
            self._parse_text_message(payload)

    def _on_open(self, ws):
        # Resubscribe if its reconnect
        if not self._is_first_connect:
            self.resubscribe()

        # Set first connect to false once its connected first time
        self._is_first_connect = False

        if self.on_open:
            return self.on_open(self)

    def _on_reconnect(self, attempts_count):
        if self.on_reconnect:
            return self.on_reconnect(self, attempts_count)

    def _on_noreconnect(self):
        if self.on_noreconnect:
            return self.on_noreconnect(self)

    def _parse_text_message(self, payload):
        """Parse text message."""
        # Decode unicode data
        if not six.PY2 and type(payload) == bytes:
            payload = payload.decode("utf-8")

        try:
            data = json.loads(payload)
        except ValueError:
            return

        # Order update callback
        if self.on_order_update and data.get("type") and data.get("data"):
            self.on_order_update(self, data)

    def _preload_instruments(self) -> None:
        """Load the InstrumentManager synchronously before the WS connects.

        This is what guarantees that ticker enrichment is available on the
        very first tick. The InstrumentManager has an in-memory + disk cache,
        so this call returns instantly on every connect after the day's first.

        Failure modes (network down, read-only filesystem, etc.) are logged
        but do not block the websocket — the feed will simply emit ticks
        with ``exchange``/``token`` only until a future ``load()`` succeeds.
        """
        mgr = self.instruments
        if mgr is None or mgr.is_loaded:
            return
        try:
            mgr.load()
        except Exception as exc:
            log.warning(
                "Instrument preload failed; tick ticker enrichment will be "
                "skipped until a subsequent successful load. Error: %s",
                exc,
            )

    def _enrich_with_ticker(self, record: dict) -> dict:
        """Add a ``ticker`` field to a tick record.

        The binary tick wire format carries only ``exchange`` + ``token`` — the
        server never sends a ticker string. This method resolves the ticker
        client-side via the attached ``InstrumentManager``, so users who
        subscribed by ticker get tick callbacks tagged with the same ticker
        without any wire-format change.

        Non-blocking and silent on failure — a missing instrument or unloaded
        manager just leaves the record alone (no exception, no lazy-load).
        """
        mgr = self.instruments
        if mgr is None:
            return record
        try:
            ticker = mgr.get_ticker_or_none(record["exchange"], record["token"])
        except Exception:
            return record
        if ticker:
            record["ticker"] = ticker
        return record

    def _parse_binary(self, bin):
        """Parse binary data to a (list of) ticks structure."""
        packets = self._split_packets(bin)  # split data to individual ticks packet
        data = []

        for packet in packets:
            if len(packet) == 22:
                format_string = "<10sid"
                exchange, token, last_trade_price = struct.unpack(format_string, packet)
                exchange = exchange.decode("utf-8").rstrip('\x00')
                data.append(self._enrich_with_ticker({
                    "exchange" : exchange,
                    "token": token,
                    "last_trade_price": last_trade_price
                }))
            elif len(packet) == 62:
                format_string = "<10sididdddi"
                exchange, token, last_trade_price, last_trade_time, open_price, high_price, low_price, close_price, volume = struct.unpack(format_string, packet)
                exchange = exchange.decode("utf-8").rstrip('\x00')
                data.append(self._enrich_with_ticker({
                    "exchange" : exchange,
                    "token": token,
                    "last_trade_price": last_trade_price,
                    "last_trade_time": last_trade_time,
                    "open_price": open_price,
                    "high_price": high_price,
                    "low_price": low_price,
                    "close_price": close_price,
                    "volume": volume
                }))
            elif len(packet) == 266:
                format_string = "<10sididdddiiidqqidiidiidiidiidiidiidiidiidiidiiii"
                unpacked_data = struct.unpack(format_string, packet)
                exchange = unpacked_data[0].decode("utf-8").rstrip('\x00')
                data.append(self._enrich_with_ticker({
                    "exchange" : exchange,
                    "token": unpacked_data[1],
                    "last_trade_price": unpacked_data[2],
                    "last_trade_time": unpacked_data[3],
                    "open_price": unpacked_data[4],
                    "high_price": unpacked_data[5],
                    "low_price": unpacked_data[6],
                    "close_price": unpacked_data[7],
                    "volume": unpacked_data[8],
                    "last_update_time": unpacked_data[9],
                    "last_trade_quantity": unpacked_data[10],
                    "average_trade_price": unpacked_data[11],
                    "total_buy_quantity": unpacked_data[12],
                    "total_sell_quantity": unpacked_data[13],
                    "open_interest": unpacked_data[14],
                    
                    "depth": {
                        "buy": [{
                            "price": unpacked_data[15],
                            "quantity": unpacked_data[16],
                            "orders": unpacked_data[17],
                        },{
                            "price": unpacked_data[18],
                            "quantity": unpacked_data[19],
                            "orders": unpacked_data[20],
                        },{
                            "price": unpacked_data[21],
                            "quantity": unpacked_data[22],
                            "orders": unpacked_data[23],
                        },{
                            "price": unpacked_data[24],
                            "quantity": unpacked_data[25],
                            "orders": unpacked_data[26],
                        },{
                            "price": unpacked_data[27],
                            "quantity": unpacked_data[28],
                            "orders": unpacked_data[29],
                        }],
                        "sell": [{
                            "price": unpacked_data[30],
                            "quantity": unpacked_data[31],
                            "orders": unpacked_data[32],
                        },{
                            "price": unpacked_data[33],
                            "quantity": unpacked_data[34],
                            "orders": unpacked_data[35],
                        },{
                            "price": unpacked_data[36],
                            "quantity": unpacked_data[37],
                            "orders": unpacked_data[38],
                        },{
                            "price": unpacked_data[39],
                            "quantity": unpacked_data[40],
                            "orders": unpacked_data[41],
                        },{
                            "price": unpacked_data[42],
                            "quantity": unpacked_data[43],
                            "orders": unpacked_data[44],
                        }]
                    }, 
                    "dpr_high": unpacked_data[45],
                    "dpr_low": unpacked_data[46],
                }))
        return data

    def _unpack_int(self, bin, start, end, byte_format="H"):
        """Unpack binary data as unsgined interger."""
        return struct.unpack("<" + byte_format, bin[start:end])[0]

    def _split_packets(self, bin):
        """Split the data to individual packets """
        # Ignore heartbeat data.
        if len(bin) < 2:
            return []

        number_of_packets = self._unpack_int(bin, 0, 2, byte_format="H")
        packets = []

        j = 2
        for i in range(number_of_packets):
            packet_length = self._unpack_int(bin, j, j + 2, byte_format="H")
            packets.append(bin[j + 2: j + 2 + packet_length])
            j = j + 2 + packet_length

        return packets