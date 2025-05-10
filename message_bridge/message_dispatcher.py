import requests
import json

from abc import ABC, abstractmethod
from .message_broker import  build_message_broker
from .circuit_breaker import CircuitBreaker






class MessageSender(ABC):

    @abstractmethod
    def send(self, message: str) -> None:
        pass


class MessageDispatcher(ABC):

    @abstractmethod
    def dispatch(self, message: str) -> None:
        pass


class RestApiSender(MessageSender):
    """
    TR: Mesajları belirtilen bir REST API uç noktasına POST eder.
    EN: Sends messages to a specified REST API endpoint via POST.
    """

    def __init__(self,
                 endpoint_url: str,
                 timeout: int = 5,
                 headers: dict = None,
                 fail_max: int = 3,
                 reset_timeout: int = 10):
        self.endpoint_url = endpoint_url
        self.timeout = timeout
        default_headers = {"Content-Type": "application/json"}
        self.headers = {**default_headers, **(headers or {})}
        self.circut_breaker = CircuitBreaker(fail_max=fail_max,
                                             reset_timeout=reset_timeout)

    def send(self, message: str) -> None:

        def request():
            response = requests.post(self.endpoint_url,
                                     json=json.loads(message),
                                     headers=self.headers,
                                     timeout=self.timeout)
            response.raise_for_status()
            return response

        try:
            return self.circut_breaker.call(request)
        except requests.exceptions.RequestException as e:
            raise RuntimeError(
                f"Failed to send message (circuit state: {self.circut_breaker.state}) to {self.endpoint_url}: {e}"
            ) from e






class SenderBuilder:

    def __init__(self) -> None:
        self._sender_type = None
        self._url = None
        self._headers = {}
        self._host = None
        self._port = None
        self._user = None
        self._password = None
        self._broker = None
        self._message = None
        self._target_queue_list = None
        self._exchange_name = "result_exchange"

    def with_sender_type(self, sender_type: str):
        self._sender_type = sender_type
        return self

    def with_url(self, url: str):
        self._url = url
        return self

    def with_headers(self, headers: dict):
        self._headers = headers
        return self

    def with_host(self, host: str):
        self._host = host
        return self

    def with_port(self, port: int):
        self._port = port
        return self

    def with_user(self, user: str):
        self._user = user
        return self

    def with_password(self, password: str):
        self._password = password
        return self

    def with_broker(self, broker: str):
        self._broker = broker
        return self

    def with_message(self, message: str):
        self._message = message
        return self

    def with_target_queue_list(self, target_queue: list):
        self._target_queue_list = target_queue
        return self

    def with_exchange_name(self, exchange_name: str):
        self._exchange_name = exchange_name
        return self

    def build(self):
        if self._sender_type == "api":
            if not self._url:
                raise ValueError("URL is required for API sender.")
            return RestApiSender(endpoint_url=self._url,headers=self._headers)

        elif self._sender_type == "broker":
            if not self._broker:
                raise ValueError(
                    "Broker type and target queue list are required.")
            return build_message_broker(broker_type=self._broker,
                                        host=self._host,
                                        port=self._port,
                                        user=self._user,
                                        password=self._password)

        else:
            raise ValueError("Invalid sender type specified.")
