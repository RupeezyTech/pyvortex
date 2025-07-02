import six
import sys
import time
import json
import struct
import logging
import threading
from twisted.internet import reactor, ssl
from twisted.python import log as twisted_log
from twisted.internet.protocol import ReconnectingClientFactory
from autobahn.twisted.websocket import WebSocketClientProtocol, \
    WebSocketClientFactory, connectWS

from .__version__ import __version__, __name__

log = logging.getLogger(__name__)

class ClientProtocol(WebSocketClientProtocol): 
    """
    A WebSocket client protocol that implements ping-pong keepalive.

    Args:
        PING_INTERVAL: The interval in seconds between sending pings.
        KEEPALIVE_INTERVAL: The interval in seconds after which a connection is considered dead if no pongs have been received.
    """
    PING_INTERVAL = 2.5
    KEEPALIVE_INTERVAL = 5

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
            if last_pong_diff > (2 * self.PING_INTERVAL):
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

    def __init__(self, access_token: str, websocket_endpoint="wss://wire.rupeezy.in/ws",reconnect=True, reconnect_max_tries=RECONNECT_MAX_TRIES, reconnect_max_delay=RECONNECT_MAX_DELAY,
                 connect_timeout=CONNECT_TIMEOUT, debug = False) -> None:
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
        # Init WebSocket client factory
        self._create_connection(self.socket_url,
                                useragent=self._user_agent())

        # Set SSL context
        context_factory = None
        if self.factory.isSecure and not disable_ssl_verification:
            context_factory = ssl.ClientContextFactory()

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

    def subscribe(self, exchange: str,token: int,mode: str)->bool:
        """
        Subscribe to a list of instrument_tokens.
        - `instrument_tokens` is list of instrument instrument_tokens to subscribe
        """
        try:
            self.ws.sendMessage(six.b(json.dumps({"message_type": self._message_subscribe, "exchange": exchange,"token": token,"mode": mode})))     

            try: 
                self.subscribed_tokens[exchange][token] = mode
            except KeyError: 
                self.subscribed_tokens[exchange] = {}
                self.subscribed_tokens[exchange][token] = mode

            return True
        except Exception as e:
            self._close(reason="Error while subscribe: {}".format(str(e)))
            raise

    def unsubscribe(self, exchange: str,token: int)->bool:
        """
        Unsubscribe the given list of instrument_tokens.
        - `instrument_tokens` is list of instrument_tokens to unsubscribe.
        """
        try:
            self.ws.sendMessage(six.b(json.dumps({"message_type": self._message_unsubscribe, "exchange": exchange,"token": token})))            

            try: 
                del(self.subscribed_tokens[exchange][token])
            except KeyError: 
                pass 

            return True
        except Exception as e:
            self._close(reason="Error while unsubscribe: {}".format(str(e)))
            raise

    def resubscribe(self):
        """Resubscribe to all current subscribed tokens."""
        modes = {}

        for exchange in self.subscribed_tokens: 
            for token in self.subscribed_tokens[exchange]: 
                self.subscribe(exchange=exchange, token=token,mode=self.subscribed_tokens[exchange][token])

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

    def _parse_binary(self, bin):
        """Parse binary data to a (list of) ticks structure."""
        packets = self._split_packets(bin)  # split data to individual ticks packet
        data = []

        for packet in packets:
            if len(packet) == 22:
                format_string = "<10sid"
                exchange, token, last_trade_price = struct.unpack(format_string, packet)
                exchange = exchange.decode("utf-8").rstrip('\x00')
                data.append({
                    "exchange" : exchange, 
                    "token": token,
                    "last_trade_price": last_trade_price
                })
            elif len(packet) == 62: 
                format_string = "<10sididdddi"
                exchange, token, last_trade_price, last_trade_time, open_price, high_price, low_price, close_price, volume = struct.unpack(format_string, packet)
                exchange = exchange.decode("utf-8").rstrip('\x00')
                data.append({
                    "exchange" : exchange, 
                    "token": token,
                    "last_trade_price": last_trade_price,
                    "last_trade_time": last_trade_time,
                    "open_price": open_price,
                    "high_price": high_price,
                    "low_price": low_price,
                    "close_price": close_price,
                    "volume": volume
                })
            elif len(packet) == 266:
                format_string = "<10sididdddiiidqqidiidiidiidiidiidiidiidiidiidiiii"
                unpacked_data = struct.unpack(format_string, packet)
                exchange = unpacked_data[0].decode("utf-8").rstrip('\x00')
                data.append({
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
                })
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