import hashlib
import hmac
import time
import urllib.parse

from hummingbot.connector.exchange.coincheck.coincheck_constants import PRIVATE_API_VERSION, REST_URL
from hummingbot.core.web_assistant.auth import AuthBase
from hummingbot.core.web_assistant.connections.data_types import RESTRequest, WSRequest


class CoincheckAuth(AuthBase):
    def __init__(self, access_key: str, secret_key: str):
        self.access_key = access_key
        self.secret_key = secret_key
        self._nonce = int(time.time() * 1000)  # Using milliseconds, similar to Date.now() in JavaScript

    def _get_nonce(self) -> str:
        """
        Generate a new nonce for each request (incremented in milliseconds).
        """
        self._nonce += 1  # Increment the nonce to avoid duplication
        return str(self._nonce)

    def _get_signature(self, message: str) -> str:
        """
        Create the HMAC-SHA256 signature for the message.
        """
        signature = hmac.new(
            bytes(self.secret_key.encode()),  # Secret key for HMAC-SHA256
            bytes(message.encode()),  # Message to hash (nonce + full URL)
            hashlib.sha256  # Hashing algorithm
        ).hexdigest()
        return signature

    def get_headers(self, request_path: str = "") -> dict:
        """
        Generate the required headers for the authenticated request.
        """
        # Construct the full URL for the request
        uri = REST_URL + PRIVATE_API_VERSION + request_path

        # The message consists of the nonce and the full URL
        message = self._get_nonce() + urllib.parse.urlparse(uri).geturl()

        # Generate headers with the HMAC-SHA256 signature
        headers = {
            'ACCESS-KEY': self.access_key,
            'ACCESS-NONCE': self._nonce,  # Pass the nonce as a string
            'ACCESS-SIGNATURE': self._get_signature(message),
            'Content-Type': 'application/json'
        }
        return headers

    async def rest_authenticate(self, request: RESTRequest) -> RESTRequest:
        """
        Adds authentication details (nonce and signature) to the REST request headers.
        """
        headers = {}
        if request.headers is not None:
            headers.update(request.headers)

        # Update headers with authentication information
        headers.update(self.get_headers(request.url))

        request.headers = headers

        return request

    async def ws_authenticate(self, request: WSRequest) -> WSRequest:
        """
        Coincheck does not require WebSocket authentication, so this is a pass-through function.
        """
        return request
