import requests
import csv
import datetime
import logging
from enum import Enum
import inspect
import wrapt
import hashlib

class Constants: 
    """
    Constants used in the API
    """
    class ExchangeTypes(str,Enum): 
        """
        Constants for exchanges
        """
        NSE_FO = "NSE_FO"
        BSE_FO = "BSE_FO"
        NSE_EQUITY = "NSE_EQ"
        BSE_EQUITY = "BSE_EQ"
        NSE_CURRENCY = "NSE_CD"
        MCX = "MCX_FO"

        def __str__(self):
            return str(self.value)
    class VarietyTypes(str,Enum): 
        """
        Constants for varieties
        """
        REGULAR_LIMIT_ORDER = "RL"
        REGULAR_MARKET_ORDER = "RL-MKT"
        STOP_LIMIT_ORDER = "SL"
        STOP_MARKET_ORDER = "SL-MKT"

        def __str__(self):
            return str(self.value)
    class ProductTypes(str,Enum): 
        """
        Constants for product types
        """
        INTRADAY = "INTRADAY"
        DELIVERY = "DELIVERY"
        MTF = "MTF"

        def __str__(self):
            return str(self.value)
    class ValidityTypes(str,Enum): 
        """
        Constants for validity types
        """
        FULL_DAY = "DAY"
        IMMEDIATE_OR_CANCEL = "IOC"
        AFTER_MARKET = "AMO"

        def __str__(self):
            return str(self.value)
    class TransactionSides(str,Enum): 
        """
        Constants for transaction sides
        """
        BUY = "BUY"
        SELL = "SELL"
        def __str__(self):
            return str(self.value)
    class QuoteModes(str,Enum): 
        """
        Constants for quote modes
        """
        LTP = "ltp"
        FULL = "full"
        OHLCV = "ohlcv"
        def __str__(self):
            return str(self.value)
    class OrderMarginModes(str,Enum): 
        """
        Constants for order margin modes
        """
        NEW_ORDER = "NEW"
        MODIFY_ORDER = "MODIFY"

        def __str__(self):
            return str(self.value)
    class Resolutions(str,Enum): 
        """
        Constants for resolutions
        """
        MIN_1 = "1"
        MIN_2 = "2"
        MIN_3 = "3"
        MIN_4 = "4"
        MIN_5 = "5"
        MIN_10 = "10"
        MIN_15 = "15"
        MIN_30 = "30"
        MIN_45 = "45"
        MIN_60 = "60"
        MIN_120 = "120"
        MIN_180 = "180"
        MIN_240 = "240"
        DAY = "1D"
        WEEK = "1W"
        MONTH = "1M"

        def __str__(self):
            return str(self.value)

def validate_inputs(func):
    @wrapt.decorator
    def wrapper(wrapped, instance, args, kwargs):
        # Get the function signature
        sig = inspect.signature(wrapped)
        # Validate the input arguments
        for param_name, param in sig.parameters.items():
            if param_name == 'self':
                continue
            param_type = param.annotation
            if param_type is inspect.Parameter.empty:
                continue
            if param_type is not inspect.Parameter.empty:
                if kwargs.get(param_name) != None: 
                    value = kwargs.get(param_name)
                else: 
                    value = args[list(sig.parameters.values()).index(param)]
                # value = args[list(sig.parameters.values()).index(param)] if param.kind == inspect.Parameter.POSITIONAL_ONLY else kwargs.get(param_name)
                if isinstance(param_type, Enum) and value not in param_type.__members__.values():
                    raise ValueError(f'{param.name} must be one of {list(param_type.__members__.values())}')
                if not isinstance(value, param_type):
                    raise TypeError(f'{param_name} must be of type {param_type.__name__}')
        sig.parameters.values()
        # Call the wrapped function
        return wrapped(*args, **kwargs)

    return wrapper(func)

def validate_selected_methods(method_names):
    def decorator(cls):
        # Iterate over the instance methods of the class
        for attr_name in dir(cls):
            if attr_name not in method_names:
                continue
            attr_value = getattr(cls, attr_name)
            if callable(attr_value) and not attr_name.startswith('__'):
                # Wrap the instance method with the validation decorator
                setattr(cls, attr_name, validate_inputs(attr_value))

        return cls

    return decorator

@validate_selected_methods(['login','place_order','modify_order','cancel_order','get_order_margin','historical_candles','quotes'])
class VortexAPI:

    def __init__(self, api_key: str, application_id: str, base_url: str = "https://vortex-api.rupeezy.in/v2",enable_logging: bool=False) -> None:
        """
        Constructor method for VortexAPI class.

        Args:
            api_key (str): API key for the Vortex API.
            api_secret (str): API secret for the Vortex API.
            base_url (str, optional): Base URL for the Vortex API. Defaults to "https://vortex-api.rupeezy.in/v2".
        """
        self.api_key = api_key
        self.application_id = application_id
        self.base_url = base_url
        self.access_token = None
        self.enable_logging = enable_logging
        if self.enable_logging:
            logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    def _make_api_request(self, method: str, endpoint: str, data: dict = None, params=None) -> dict:
        """
        Private method to make HTTP requests to the Vortex API.

        Args:
            method (str): HTTP method for the request (e.g. "GET", "POST", "PUT", "DELETE").
            endpoint (str): API endpoint for the request.
            data (dict, optional): Payload data for the request. Defaults to None.

        Returns:
            dict: Dictionary containing the response data from the API.
        """
        if(self.access_token == None):
            op = {}
            op["status"]= "error"
            op["message"] = "please login first"
            return op
        bearer_token = f"Bearer {self.access_token}"
        headers = {"Content-Type": "application/json", "Authorization": bearer_token}
        url = self.base_url + endpoint
        if self.enable_logging:
            logging.debug(f"Making network call to {url}  , params: {params}, data: {data}, headers: {headers}")
        response = requests.request(method, url, headers=headers, json=data,params=params)
        if self.enable_logging:
            logging.debug(f"Response received from {url}  , body: {response.json()}")
        response.raise_for_status()
        return response.json()
    
    def _make_unauth_request(self, method: str, endpoint: str, data: dict = None, params: dict = None) -> dict:
        """
        Private method to make HTTP requests to the Vortex API.

        Args:
            method (str): HTTP method for the request (e.g. "GET", "POST", "PUT", "DELETE").
            endpoint (str): API endpoint for the request.
            data (dict, optional): Payload data for the request. Defaults to None.

        Returns:
            dict: Dictionary containing the response data from the API.
        """
        headers = {"Content-Type": "application/json", "x-api-key": self.api_key}
        url = self.base_url + endpoint
        if self.enable_logging:
            logging.debug(f"Making network call to {url}  , params: {params}, data: {data}, headers: {headers}")
        response = requests.request(method, url, headers=headers, json=data)
        response.raise_for_status()
        if self.enable_logging:
            logging.debug(f"Response received from {url}  , body: {response.json()}")
        return response.json()
    
    def login(self, client_code: str, password: str, totp: str)->dict:
        """
        Depricating Soon. Use SSO Login instead. Login using password and totp directly
        
        Documentation:
            https://vortex.rupeezy.in/docs/authentication/

        Args:
            client_code(str): Client Code of the account
            password(str): Password of the account
            totp(str): TOTP generated using third party apps like google authenticator etc. 

        Returns:
            dict: JSON response containing the details of the user
        """
        endpoint = "/user/login"
        data = {
            "client_code": client_code,
            "password": password,
            "totp": totp,
            "application_id": self.application_id
        }
        res = self._make_unauth_request("POST", endpoint= endpoint, data=data)
        self._setup_client_code(login_object=res)
        return res
    
    def download_master(self) -> dict:
        """
        Download list of all available instruments and their details across all exchanges

        Documentation:
            https://vortex.rupeezy.in/docs/historical/#instrument-list

        Returns:
            dict: CSV Array of all instruments. The first row contains headers
        """
        endpoint = "/data/instruments"
        bearer_token = f"Bearer {self.access_token}"
        headers = {"Content-Type": "application/json", "Authorization": bearer_token}
        with requests.Session() as s: 
            download = s.get(url=self.base_url + endpoint,headers=headers)
            decoded_content = download.content.decode('utf-8')
            cr = csv.reader(decoded_content.splitlines(), delimiter=',')
            my_list = list(cr)
            return my_list
    
    def place_order(self,exchange: Constants.ExchangeTypes, token: int, transaction_type: Constants.TransactionSides, product: Constants.ProductTypes, variety: Constants.VarietyTypes, 
                quantity: int, price: float, trigger_price: float, disclosed_quantity: int, validity: Constants.ValidityTypes) -> dict:
        """
        Place an order for a specific security

        Documentation:
            https://vortex.rupeezy.in/docs/order/#placing-an-order

        Args:
            exchange (Constants.ExchangeTypes): Possible values: [NSE_EQ, NSE_FO, BSE_EQ, BSE_FO, NSE_CD or MCX_FO]
            token (int): Security token of the scrip. It can be found in the scripmaster file
            transaction_type (Constants.TransactionSides): Possible values: [BUY, SELL]
            product (Constants.ProductTypes): Possible values: [INTRADAY, DELIVERY, MTF]. MTF product can only be used in NSE_EQ exchange.
            variety (Constants.VarietyTypes): Possible values: [RL, RL-MKT, SL, SL-MKT]. RL means regular orders, SL means Stop Loss order. 
                        MKT means that the trade will happen at market price
            quantity (int): For exchange NSE_FO, if you want to trade in 2 lots and lot size is 50, you should pass 100. 
                            In all other exchanges, you should pass just the number of lots. For example, in MCX_FO, 
                            if you want to trade 5 lots, you should pass just 5.
            price (float): Price should be an integer multiple of Tick Size. For example, IDEA's tick size is 0.05. 
                        So the price entered can be 9.5 or 9.65. It cannot be 9.67. 
                        In case of market orders, you should send the Last Trade Price received from the Quote API or Websocket API
            trigger_price (float): To be used for Stop loss orders. For BUY side SL orders, trigger_price should be 
                                lesser than price. for SELL side SL orders, trigger_price should be greater than price.
            disclosed_quantity (int): Can be any number lesser than or equal to quantity, including 0
            validity (Constants.ValidityTypes): Can be DAY for orders which are valid throughout the day, or IOC. 
                            IOC order will be cancelled if it is not traded immediately
        Returns:
            dict: JSON response containing the details of the placed order

        Raises:
            HTTPError: If any HTTP error occurs during the API call
        """

        endpoint = "/trading/orders/regular"
        if validity == Constants.ValidityTypes.FULL_DAY: 
            validity_days = 1 
            is_amo = False
        elif validity == Constants.ValidityTypes.IMMEDIATE_OR_CANCEL: 
            validity_days = 0 
            is_amo = False
        else: 
            validity_days = 1 
            is_amo = True 

        data = {
            "exchange": exchange,
            "token": token,
            "transaction_type": transaction_type,
            "product": product,
            "variety": variety,
            "quantity": quantity,
            "price": price,
            "trigger_price": trigger_price,
            "disclosed_quantity": disclosed_quantity,
            "validity": validity,
            "validity_days": validity_days,
            "is_amo": is_amo
        }
        
        return self._make_api_request("POST", endpoint, data=data)
    
    def modify_order(self, order_id: str, variety: Constants.VarietyTypes, quantity: int, traded_quantity: int, price: float, trigger_price: float, disclosed_quantity: int, validity: Constants.ValidityTypes) -> dict:
        """
        Method to modify an order using the Vortex API.

        Documentation:
            https://vortex.rupeezy.in/docs/order/#modifying-an-order

        Args:
            exchange (Constants.ExchangeTypes): Possible values: [NSE_EQ, NSE_FO, BSE_EQ, BSE_FO, NSE_CD or MCX_FO]
            order_id (str): The unique ID of the order to modify.
            variety (Constants.VarietyTypes): Possible values: [RL, RL-MKT, SL, SL-MKT]. RL means regular orders, SL means Stop Loss order. 
                    MKT means that the trade will happen at market price
            quantity (int): The new quantity for the order.
            traded_quantity (int): The quantity of the order that has already been traded.
            price (float): The new price for the order.
            trigger_price (float): The new trigger price for the order. Required for SL and SL-M orders.
            disclosed_quantity (int): The new quantity to be disclosed publicly.
            validity (Constants.ValidityTypes): The new validity for the order (e.g. DAY, IOC, GTD).

        Returns:
            dict: Dictionary containing the response data from the API.
        """

        endpoint = f"/trading/orders/regular/{order_id}"
        if validity == Constants.ValidityTypes.FULL_DAY: 
            validity_days = 1 
        elif validity == Constants.ValidityTypes.IMMEDIATE_OR_CANCEL: 
            validity_days = 0 
        else: 
            validity_days = 1 

        data = {
            "variety": variety,
            "quantity": quantity,
            "traded_quantity": traded_quantity,
            "price": price,
            "trigger_price": trigger_price,
            "disclosed_quantity": disclosed_quantity,
            "validity": validity,
            "validity_days": validity_days
        }
        return self._make_api_request("PUT", endpoint, data=data)
    
    def cancel_order(self, order_id: str) -> dict:
        """
        Method to cancel an order using the Vortex API.

        Documentation:
            https://vortex.rupeezy.in/docs/order/#cancel-an-order

        Args:
            exchange (Constants.ExchangeTypes): Possible values: [NSE_EQ, NSE_FO, BSE_EQ, BSE_FO, NSE_CD or MCX_FO]
            order_id (str): The unique ID of the order to cancel.

        Returns:
            dict: Dictionary containing the response data from the API.
        """
        
        endpoint = f"/trading/orders/regular/{order_id}"
        return self._make_api_request("DELETE", endpoint)

    def orders(self,limit: int, offset: int) -> dict:
        """
        Method to get all orders.

        Documentation:
            https://vortex.rupeezy.in/docs/order/#fetching-order-book

        Args:
            limit (int): Limit is the number of orders to be fetched. 
            offset (int): Offset should atleast be 1 

        Returns:
            dict: Dictionary containing the response data from the API.
        """
        endpoint = f"/trading/orders?limit={limit}&offset={offset}"
        return self._make_api_request("GET", endpoint)
    
    def order_history(self,order_id: str) -> dict:
        """
        Method to get the order history of a particular order

        Documentation:
            https://vortex.rupeezy.in/docs/order/

        Args:
            order_id (str): Order id for which history has to be fetched

        Returns:
            dict: Dictionary containing the response data from the API.
        """
        endpoint = f"/trading/orders/{order_id}"
        return self._make_api_request("GET", endpoint)

    def positions(self) -> dict:
        """
        Method to get the position book using the Vortex API.

        Documentation:
            https://vortex.rupeezy.in/docs/positions/#fetch-all-positions

        Returns:
            dict: Dictionary containing the response data from the API.
        """
        endpoint = f"/trading/portfolio/positions"
        return self._make_api_request("GET", endpoint)
    
    def holdings(self) -> dict:
        """
        Method to get the holdings of the user using the Vortex API.

        Documentation:    
            https://vortex.rupeezy.in/docs/holdings/

        Returns:
            dict: Dictionary containing the response data from the API.
        """
        endpoint = "/trading/portfolio/holdings"
        return self._make_api_request("GET", endpoint)
    
    def trades(self) -> dict:
        """
        Method to get today's trades of the user using the Vortex API.

        Documentation:    
            https://vortex.rupeezy.in/docs/positions/#get-trades

        Returns:
            dict: Dictionary containing the response data from the API.
        """
        endpoint = "/trading/trades"
        return self._make_api_request("GET", endpoint)
    
    def funds(self) -> dict:
        """
        Method to get the funds of the user using the Vortex API.

        Documentation:    
            https://vortex.rupeezy.in/docs/user/#available-funds

        Returns:
            dict: Dictionary containing the response data from the API.
        """
        endpoint = "/user/funds"
        return self._make_api_request("GET", endpoint)
    
    def get_order_margin(self, exchange: Constants.ExchangeTypes, token: int, transaction_type: Constants.TransactionSides, product: Constants.ProductTypes, variety: Constants.VarietyTypes, 
                     quantity: int, price: float,mode: Constants.OrderMarginModes, old_quantity: int = 0 , old_price: float = 0 ) -> dict:
        """
        Get the margin required for placing an order for a specific security.

        Documentation:    
            https://vortex.rupeezy.in/docs/margin/#order-margin

        Args:
            exchange (Constants.ExchangeTypes): Possible values: [NSE_EQ, NSE_FO, BSE_EQ, BSE_FO, NSE_CD or MCX_FO]
            token (int): Security token of the scrip. It can be found in the scripmaster file
            transaction_type (Constants.TransactionSides): Possible values: [BUY, SELL]
            product (Constants.ProductTypes): Possible values: [INTRADAY, DELIVERY, MTF]. MTF product can only be used in NSE_EQ exchange.
            variety (Constants.VarietyTypes): Possible values: [RL, RL-MKT, SL, SL-MKT]. RL means regular orders, SL means Stop Loss order. 
                        MKT means that the trade will happen at market price
            quantity (int): For exchange NSE_FO, if you want to trade in 2 lots and lot size is 50, you should pass 100. 
                            In all other exchanges, you should pass just the number of lots. For example, in MCX_FO, 
                            if you want to trade 5 lots, you should pass just 5.
            price (float): Price should be an integer multiple of Tick Size. For example, IDEA's tick size is 0.05. 
                        So the price entered can be 9.5 or 9.65. It cannot be 9.67. 
                        In case of market orders, you should send the Last Trade Price received from the Quote API or Websocket API
            mode (Constants.OrderMarginModes): Possible values: [NEW, MODIFY] , Whether you are trying to modify an existing order or placing a new order.            
            old_quantity (int): For NSE_FO segments, old_quantity is lots * lot_size. For all others, enter just the number of lots. Required if mode is MODIFY
            old_price (float): Old Price in INR. Required if mode is MODIFY

        Returns:
            dict: JSON response containing the details of the margin required to place the order

        Raises:
            HTTPError: If any HTTP error occurs during the API call
        """
        
        endpoint = "/margins/order"
        
        data = {
            "exchange": exchange,
            "token": token,
            "transaction_type": transaction_type,
            "product": product,
            "variety": variety,
            "quantity": quantity,
            "price": price,
            "old_quantity": old_quantity,
            "old_price": old_price,
            "mode": mode,
        }
        return self._make_api_request("POST", endpoint, data=data)
    
    def quotes(self, instruments: list, mode: Constants.QuoteModes)-> dict: 
        """
        Gets quotes of up to 1000 instruments at a time. 

        Documentation:    
            https://vortex.rupeezy.in/docs/historical/#fetch-price-quotes

        Args:
            instrument(list): List of instruments. The items should be like ( "NSE_EQ-22", "NSE_FO-1234")
            mode(Constants.QuoteModes): Quote mode. Can be ["ltp","ohlcv", "full"]. LTP quotes just give the last trade price, ohlc give open, high, low, close and volume, full mode also gives depth.

        Returns:
            dict: JSON response containing quotes. It is possible that not all the symbol identifiers you passed had a quote available. Those inputs will be missing from the response.
            Also, the order of output might be different than the order of input
        """
        endpoint = "/data/quotes"
        params = {"q": instruments,"mode": mode}
        return self._make_api_request("GET", endpoint, data=None,params=params)
    
    def historical_candles(self, exchange: Constants.ExchangeTypes, token: int, to: datetime.datetime , start: datetime.datetime, resolution: Constants.Resolutions): 
        """
        Gets historical candle data of a particular instrument. 

        Documentation:    
            https://vortex.rupeezy.in/docs/historical/#fetch-historical-candle-data

        Args:
            exchange (Constants.ExchangeTypes): Possible values: [NSE_EQ, NSE_FO, BSE_EQ, BSE_FO, NSE_CD or MCX_FO]
            token (int): Security token of the scrip. It can be found in the instruments master file: 
            to (datetime): datetime up till when you want to receive candles 
            start (datetime): datetime from when you want to receive candles 
            resolution (Constants.Resolutions): resoulution of the candle. can be "1", "2", "3", "4", "5", "10", "15", "30", "45", "60", "120", "180", "240", "1D", "1W", "1M"
        
        Returns:
            dict: JSON response containing the historical candles
        """

        if not isinstance(token, int):
            raise TypeError("token must be an integer")
        if not isinstance(to,datetime.datetime): 
            raise TypeError("to must be a datetime")
        if not isinstance(start,datetime.datetime): 
            raise TypeError("start must be a datetime")


        endpoint = "/data/history"
        params = {"exchange": exchange,"token": token , "to": int(to.timestamp()), "from": int(start.timestamp()), "resolution": resolution}
        return self._make_api_request("GET", endpoint, data=None,params=params)

    def login_url(self, callback_param: str) -> str:
        """
        Returns the login URL for the Vortex API.

        Documentation:
            https://vortex.rupeezy.in/docs/authentication/

        Returns:
            str: The login URL for the Vortex API.
        """

        return f"https://flow.rupeezy.in?applicationId={self.application_id}&cb_param={callback_param}"
    
    def exchange_token(self,auth_code: str) -> dict:
        """
        Exchange the auth code received from the login URL for an access token.

        Documentation:
            https://vortex.rupeezy.in/docs/authentication/

        Args:
            auth_code (str): The authorization code received from the login URL.

        Returns:
            dict: JSON response containing the details of the user
        """

        endpoint = "/user/session"
        data = {
            "token": auth_code,
            "applicationId": self.application_id, 
            "checksum": self._sha256_hash(f"{self.application_id}{auth_code}{self.api_key}")
        }
        res = self._make_unauth_request("POST", endpoint= endpoint, data=data)
        self._setup_client_code(login_object=res)
        return res

    def _sha256_hash(self,text: str) -> str:
        sha = hashlib.sha256()
        sha.update(text.encode('utf-8'))
        return sha.hexdigest()

    def _setup_client_code(self, login_object: dict) -> bool: 
        """ 
        Sets up access token after login

        Args: 
            login_object(dict): Login object received

        Returns: 
            (bool): Whether successful or not
        """

        if (('data' in login_object ) and login_object["data"] != None and login_object["data"]["access_token"] != None): 
            self.access_token = login_object["data"]["access_token"]
            return True
        
        return False
    
