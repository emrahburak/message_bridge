from abc import ABC, abstractmethod
import os
import redis
import pika
import time
from message_bridge.log_setup import setup_logger


class MessageBroker(ABC):
    """
    TR: Mesajlaşma broker'ı için temel soyut sınıf. Bu sınıf, mesaj alma, yollama, ack/nack işlemleri gibi temel 
    işlevleri sağlayan soyut metodlar içerir. Her broker (RabbitMQ, Redis vb.) bu sınıfı miras almalı ve metodları 
    kendine özgü bir şekilde implement etmelidir.
    
    EN: Base abstract class for message brokers. This class contains abstract methods for core operations like 
    consuming and publishing messages, performing ack/nack operations, etc. Each broker (RabbitMQ, Redis, etc.) 
    should inherit this class and implement these methods in its own way.
    """

    @abstractmethod
    def consume(self, queue_name: str):
        """
        TR: Kuyruktan bir mesaj alır. Her broker'ın bu fonksiyonu kendine özgü bir şekilde implement etmesi gerekir.

        EN: Retrieves a message from the queue. Each broker must implement this function in its own way.
        """
        pass

    @abstractmethod
    def publish(self, queue_name, message: str) -> None:
        """
        TR: Verilen kuyruğa bir mesaj yayınlar. Her broker'ın bu fonksiyonu kendine özgü bir şekilde implement etmesi gerekir.

        EN: Publishes a message to the specified queue. Each broker must implement this function in its own way.
        """
        pass

    @abstractmethod
    def ack(self, delivery_tag):
        """
        TR: Mesajın başarıyla işlendiğini onaylar.

        EN: Acknowledges that the message has been successfully processed.
        """
        pass

    @abstractmethod
    def nack(self, delivery_tag, requeue=True):
        """
        TR: Mesajı reddeder. Eğer requeue True ise, mesaj tekrar kuyruğa eklenir.

        EN: Rejects the message. If requeue is True, the message is requeued.
        """
        pass

    @abstractmethod
    def declare_queue(self, queue_name: str, durable: bool = True):
        """
        TR: Yeni bir queue oluşturur. Var olan bir kuyruksa, mevcut tanımla devam eder.
        EN: Declares a new queue. If the queue already exists, continues with the existing definition.
        """
        pass

    @abstractmethod
    def declare_exchange(self, exchange_name: str, exchange_type="direct"):
        """
        TR: Yeni bir exchange oluşturur. Her broker'ın bu fonksiyonu kendine özgü bir şekilde implement etmesi gerekir.

        EN: Declares a new exchange. Each broker must implement this function in its own way.
        """
        pass

    @abstractmethod
    def bind_queue_to_exchange(self, queue_name: str, exchange_name: str,
                               routing_key: str):
        """
        TR: Bir queue'u exchange'e bağlar. Her broker'ın bu fonksiyonu kendine özgü bir şekilde implement etmesi gerekir.

        EN: Binds a queue to an exchange. Each broker must implement this function in its own way.
        """
        pass

    @abstractmethod
    def start_consuming(self, queue_name: str, callback):
        """
        TR: Kuyruğu dinlemeye başlar. Her broker'ın bu fonksiyonu kendine özgü bir şekilde implement etmesi gerekir.

        EN: Starts consuming messages from the queue. Each broker must implement this function in its own way.
        """
        pass

    @abstractmethod
    def start_consuming_multiple(self, queue_names: list[str], callback):
        """
        TR: Belirtilen birden fazla kuyruğu dinlemeye başlar. Herhangi bir kuyruğa mesaj geldiğinde, callback fonksiyonu çağrılır.
        Her broker bu işlemi kendi altyapısına uygun şekilde uygulamalıdır.

        EN: Starts consuming messages from multiple queues. Whenever a message arrives in any of the queues,
        the callback function is invoked. Each broker must implement this according to its underlying infrastructure.
        """
        pass

    @abstractmethod
    def safe_publish(self, queue_items: list, exchange_name: str,
                     message: str):
        """
        TR: Güvenli bir şekilde mesajları birden fazla kuyruğa yayınlar. Her broker'ın bu fonksiyonu kendine özgü bir şekilde implement etmesi gerekir.

        EN: Safely publishes a message to multiple queues. Each broker must implement this function in its own way.
        """
        pass

    @abstractmethod
    def consume_configure(self,
                          queue_name: str,
                          exchange_name: str,
                          routing_key: str = None):
        """
        TR: Tüketimden önce kuyruğun, exchange'in ve bağlamanın yapılmasını zorunlu kılar.
        EN: Enforces queue, exchange, and binding configuration before consuming.
        """
        pass

    def handle_ack(self, delivery_tag):
        """
        TR: Ack stratejisini uygular. Eğer broker bir ack stratejisi belirlemişse, 
        bu stratejiye uygun olarak ack işlemi yapılır. Bu fonksiyon genel olarak mesajı 
        işlemek ve uygun stratejiyi uygulamak için kullanılır.

        EN: Applies the acknowledgment strategy. If the broker has set an ack strategy, 
        it will perform the acknowledgment according to that strategy. This function is used 
        to process the message and apply the appropriate strategy.
        """

        if hasattr(self, "ack_strategy"):
            self.ack_strategy.handle_ack(self, delivery_tag, message=None)


class AckStrategy(ABC):
    """
    TR: Tüm ack stratejileri için temel soyut sınıftır. Her strateji kendi 
    'handle_ack' yöntemini uygulamalıdır.

    EN: Abstract base class for all acknowledgment strategies. 
    Each strategy must implement the 'handle_ack' method.
    """

    @abstractmethod
    def handle_ack(self, broker, delivery_tag, message):
        """
        TR: Mesaj işleme sonucunda hangi ack stratejisinin uygulanacağını tanımlar.

        EN: Defines how the message acknowledgment should be handled
        after message processing.
        """
        pass


class AutoAckStrategy(AckStrategy):
    """
    TR: Mesaj başarıyla alındıktan sonra otomatik olarak 'ack' gönderir.
    Bu strateji, hataları takip etmeye gerek olmayan durumlar için uygundur.

    EN: Automatically sends 'ack' after the message is received.
    Suitable for scenarios where failure tracking is not necessary.
    """

    def handle_ack(self, broker, delivery_tag, message=None):
        broker.ack(delivery_tag)


class NackAndRequeueStrategy(AckStrategy):
    """
    TR: Mesaj işlenemediğinde 'nack' gönderir ve mesajı tekrar kuyruğa koyar.
    Bu strateji, mesajın yeniden denenmesini sağlar.

    EN: Sends a 'nack' when message processing fails and requeues the message.
    This allows the message to be retried.
    """

    def handle_ack(self, broker, delivery_tag, message=None):
        broker.nack(delivery_tag, requeue=True)


class NackWithoutRequeueStrategy(AckStrategy):
    """
    TR: Mesaj işlenemediğinde 'nack' gönderir ve mesajı yeniden kuyruğa koymaz.
    Bu strateji, mesajı kaybetmeyi ya da 'dead-letter' queue gibi sistemleri kullanmayı amaçlar.

    EN: Sends a 'nack' on message failure without requeuing it.
    Suitable for message discarding or use with dead-letter queues.
    """

    def handle_ack(self, broker, delivery_tag, message=None):
        broker.nack(delivery_tag, requeue=False)


class LimitedRetryNackStrategy(AckStrategy):
    """
    TR: Mesaj için belirli sayıda yeniden deneme hakkı tanır. Eğer deneme sayısı 
    sınırı aşarsa mesaj yeniden kuyruğa konmaz.

    EN: Allows a limited number of retries for a message. 
    If retry limit is exceeded, the message is not requeued.
    """

    def __init__(self, max_retries=3):
        """
        TR: max_retries - Maksimum yeniden deneme sayısı.
        EN: max_retries - Maximum number of retry attempts.
        """
        self.max_retries = max_retries

    def handle_ack(self, broker, delivery_tag, message=None):
        """
        TR: Mesajın yeniden denenip denenmeyeceğine karar verir.
        EN: Decides whether to requeue the message based on retry count.
        """
        headers = getattr(broker, "headers", {})
        retry_count = headers.get("x-retry-count", 0)

        if retry_count < self.max_retries:
            broker.nack(delivery_tag, requeue=True)
        else:
            broker.nack(delivery_tag, requeue=False)


class RedisBroker(MessageBroker):

    def __init__(self,
                 host: str = 'localhost',
                 port: int = 6379,
                 max_retries=5,
                 retry_interval=5):
        self.logger = setup_logger()
        self.host = host
        self.port = port
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.client = self._connect()

    def _connect(self):
        """Redis bağlantısını kur ve ping at """

        retries = 0
        while retries < self.max_retries:
            try:
                client = redis.StrictRedis(host=self.host,
                                           port=self.port,
                                           db=0)
                if client.ping():
                    self.logger.info("connected to Redis succefully")
                    return client
            except Exception as e:
                retries += 1
                self.logger.error(
                    f"Redis connection failed: {e}. Retrying in {self.retry_interval} seconds... (Attempt {retries}/{self.max_retries})"
                )
                time.sleep(self.retry_interval)

        raise ConnectionError(
            "Failed to connect to Redis after multiple attempts")

    def consume(self, queue_name: str) -> None:
        _, message = self.client.brpop(queue_name)
        return message.decode("utf-8")

    def publish(self, queue_name: str, message: str):
        self.client.rpush(queue_name, message)

    def safe_publish(self, queue_items: list, exchange_name: str | None,
                     message: str):
        for queue_name in queue_items:
            self.publish(queue_name=queue_name, message=message)
            self.logger.info(f"(Redis) Published to queue: {queue_name}")

    def ack(self, delivery_tag):
        # Redis'te ack işlemi yoktur.
        pass

    def nack(self, delivery_tag, requeue=True, message=None):
        # Redis'te nack işlemi yoktur.
        if requeue:
            self.client.rpush(delivery_tag, message)

    def declare_exchange(self, exchange_name: str, exchange_type="direct"):
        # Redis'te exchange kavramı yoktur.
        pass

    def bind_queue_to_exchange(self, queue_name: str, exchange_name: str,
                               routing_key: str):
        # Redis'te binding kavramı yoktur.
        pass

    def start_consuming(self, queue_name: str, callback):
        while True:
            message = self.consume(queue_name)
            if message:
                callback(None, None, None, message.encode("utf-8"))

    def start_consuming_multiple(self, queue_names: list[str], callback):
        """Birden fazla kuyruğu bloklayarak dinler."""
        self.logger.info(f"Started consuming Redis queues: {queue_names}")
        while True:
            try:
                result = self.client.brpop(queue_names)
                if result:
                    queue_name, message = result
                    callback(None, None, None, message)
            except Exception as e:
                self.logger.error(f"Error while consuming Redis queues: {e}")
                time.sleep(self.retry_interval)


class RabbitMQBroker:

    def __init__(self,
                 host="rabbitmq",
                 port=5672,
                 user="myuser",
                 password="mypassword",
                 max_retries=5,
                 retry_interval=5,
                 ack_strategy=None):
        self.logger = setup_logger()  # Logger'ı ayarla
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.ack_strategy = ack_strategy or AutoAckStrategy()
        self.max_retries = max_retries
        self.retry_interval = retry_interval
        self.connection, self.channel = self._connect()

    def _connect(self):
        """RabbitMQ bağlantısını kurar."""
        retries = 0
        while retries < self.max_retries:
            try:
                credentials = pika.PlainCredentials(self.user, self.password)
                parameters = pika.ConnectionParameters(host=self.host,
                                                       port=self.port,
                                                       credentials=credentials,
                                                       heartbeat=600)
                connection = pika.BlockingConnection(parameters)
                channel = connection.channel()
                self.logger.info("Connected to RabbitMQ successfully")
                return connection, channel
            except Exception as e:
                retries += 1
                self.logger.error(
                    f"RabbitMQ connection failed: {e}. Retrying in {self.retry_interval} seconds... (Attempt {retries}/{self.max_retries})"
                )
                time.sleep(self.retry_interval)

        raise ConnectionError(
            "Failed to connect to RabbitMQ after multiple attempts")

    def consume(self, queue_name, auto_ack=False):
        method_frame, _, body = self.channel.basic_get(queue=queue_name,
                                                       auto_ack=auto_ack)
        if method_frame:
            return method_frame, body.decode("utf-8")
        return None, None

    def publish(self, exchange, queue_name, message):
        self.channel.basic_publish(exchange=exchange,
                                   routing_key=queue_name,
                                   body=message)

    def safe_publish(self, queue_items: list, exchange_name: str,
                     message: str):
        self.declare_exchange(exchange_name=exchange_name)
        for queue_name in queue_items:
            self.channel.queue_declare(queue=queue_name, durable=True)
            self.bind_the_queue_exchange(queue_name=queue_name,
                                         exchange_name=exchange_name,
                                         routing_key=queue_name)
            self.publish(exchange=exchange_name,
                         queue_name=queue_name,
                         message=message)
            self.logger.info(f"(RabbitMQ) Published to queue: {queue_name}")

    def ack(self, delivery_tag):
        self.channel.basic_ack(delivery_tag=delivery_tag)

    def nack(self, delivery_tag, requeue=True):
        self.channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)

    def declare_queue(self, queue_name: str, durable: bool = True):
        self.channel.queue_declare(queue=queue_name, durable=durable)

    def declare_exchange(self, exchange_name, exchange_type='direct'):
        """Yeni bir exchange oluştur """
        self.channel.exchange_declare(exchange=exchange_name,
                                      exchange_type='direct',
                                      durable=True)
        self.logger.info("Exchange declared successfully")

    def bind_the_queue_exchange(self, queue_name, exchange_name, routing_key):
        """Bir queueu'y exchangeye bağla """
        self.channel.queue_bind(queue=queue_name,
                                exchange=exchange_name,
                                routing_key=routing_key)
        self.logger.info(
            f"Queue bound to exchange: queue={queue_name}, exchange={exchange_name}, routing_key={routing_key}"
        )

    def consume_configure(self,
                          queue_name: str,
                          exchange_name: str,
                          routing_key: str = None):
        """
        TR: Tüketimden önce kuyruğun, exchange'in ve bağlamanın yapılmasını sağlar.
        EN: Ensures queue, exchange, and binding configuration before consuming.
        """
        if routing_key is None:
            routing_key = queue_name

        self.declare_exchange(exchange_name=exchange_name)
        self.declare_queue(queue_name=queue_name, durable=True)
        self.bind_the_queue_exchange(queue_name=queue_name,
                                     exchange_name=exchange_name,
                                     routing_key=routing_key)

    def start_consuming(self, queue_name: str, callback):

        self.channel.basic_consume(queue=queue_name,
                                   on_message_callback=callback)
        self.logger.info("Waiting for messages...")
        self.channel.start_consuming()

    def start_consuming_multiple(self, queue_names: list[str], callback):
        """
        TR: Birden fazla kuyruğu dinlemeye başlar.
        EN: Starts consuming messages from multiple queues.
        """

        for queue in queue_names:
            self.channel.queue_declare(queue=queue, durable=True)
            self.channel.basic_consume(queue=queue,
                                       on_message_callback=callback,
                                       auto_ack=isinstance(
                                           self.ack_strategy, AutoAckStrategy))
            self.logger.info(f"Started consuming queue: {queue}")

        self.logger.info("Waiting for messages from multiple queues...")
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.channel.stop_consuming()
            self.logger.warning("Stopped consuming due to keyboard interrupt.")


def get_message_broker(broker_type: str, **kwargs) -> MessageBroker:
    """
    TR:
    Belirtilen türde bir mesaj aracısı (message broker) nesnesi döndürür.
    
    Parametreler:
        broker_type (str): 'redis' veya 'rabbitmq' gibi broker türü.
        **kwargs: Broker yapılandırması için gerekli anahtar kelime argümanları.

    Dönüş:
        MessageBroker: Uygun alt sınıftan bir mesaj aracısı nesnesi.

    Hata:
        ValueError: Geçersiz bir broker türü verildiğinde fırlatılır.

    EN:
    Returns a message broker instance based on the given type.

    Args:
        broker_type (str): The type of broker, e.g., 'redis' or 'rabbitmq'.
        **kwargs: Keyword arguments required for broker configuration.

    Returns:
        MessageBroker: An instance of the corresponding broker subclass.

    Raises:
        ValueError: If an unsupported broker type is provided.
    """

    if broker_type == "redis":
        return RedisBroker(**kwargs)
    elif broker_type == "rabbitmq":
        return RabbitMQBroker(**kwargs)
    else:
        raise ValueError(f"Invalid broker type: {broker_type}")


def build_message_broker(broker_type: str,
                         host: str,
                         port: int,
                         user: str = None,
                         password: str = None) -> MessageBroker:
    """
    TR:
    Belirtilen konfigürasyon bilgilerine göre yapılandırılmış bir mesaj aracısı (message broker) oluşturur.
    RabbitMQ için uygun ack (onay) stratejisini de ortama göre belirleyip uygular.

    Parametreler:
        broker_type (str): Broker türü ('rabbitmq' veya 'redis').
        host (str): Broker sunucu adresi.
        port (int): Broker portu.
        user (str, optional): Kullanıcı adı (RabbitMQ için gerekli olabilir).
        password (str, optional): Parola (RabbitMQ için gerekli olabilir).

    Dönüş:
        MessageBroker: Yapılandırılmış mesaj aracısı nesnesi.

    Hatalar:
        ValueError: Desteklenmeyen bir broker türü veya geçersiz bir ack stratejisi verildiğinde fırlatılır.

    EN:
    Builds and returns a configured message broker instance based on the given parameters.
    For RabbitMQ, it determines and applies the appropriate acknowledgment strategy from the environment.

    Args:
        broker_type (str): The broker type ('rabbitmq' or 'redis').
        host (str): Host address of the broker.
        port (int): Port number of the broker.
        user (str, optional): Username (may be required for RabbitMQ).
        password (str, optional): Password (may be required for RabbitMQ).

    Returns:
        MessageBroker: A properly configured broker instance.

    Raises:
        ValueError: If the broker type or the ACK strategy is unsupported.
    """

    if broker_type == "rabbitmq":
        ack_strategy_name = os.getenv("ACK_STRATEGY", "auto_ack").lower()
        strategy_map = {
            "auto_ack": AutoAckStrategy,
            "nack_and_requeue": NackAndRequeueStrategy,
            "nack_without_requeue": NackWithoutRequeueStrategy,
            "limited_retry_nack": LimitedRetryNackStrategy
        }

        if ack_strategy_name not in strategy_map:
            raise ValueError(f"Unsupported ack strategy: {ack_strategy_name}")

        ack_strategy = strategy_map[ack_strategy_name]()

        return get_message_broker(broker_type,
                                  host=host,
                                  port=port,
                                  user=user,
                                  password=password,
                                  ack_strategy=ack_strategy)

    elif broker_type == "redis":
        return get_message_broker(broker_type, host=host, port=port)

    else:
        raise ValueError(f"Unsupported broker type: {broker_type}")
