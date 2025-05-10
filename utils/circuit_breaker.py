import time


class CircuitBreaker:
    """
    TR:
    CircuitBreaker (Devre Kesici), bir fonksiyonun art arda başarısız olması durumunda,
    bir süreliğine çağrılmasını engelleyen bir hata tolerans mekanizmasıdır.
    Başarısızlık sayısı belli bir eşiği geçerse devre açılır ve sistem belirtilen süre boyunca
    bu fonksiyonu çağırmayı reddeder. Süre dolduktan sonra devre "yarı açık" hale gelir ve
    yeniden başarı durumuna göre devre kapanabilir.

    EN:
    A CircuitBreaker is a fault-tolerance mechanism that prevents a function from being called
    after it has failed a certain number of times. When the failure count exceeds the threshold,
    the circuit opens and the function is blocked for a specified timeout duration. After the
    timeout, the circuit enters a half-open state to test if recovery is possible.
    """

    def __init__(self, fail_max=3, reset_timeout=10):
        """
        TR:
        :param fail_max: Devreyi açmak için gereken maksimum hata sayısı (varsayılan: 3)
        :param reset_timeout: Devre açıldıktan sonra tekrar denenmesi için beklenecek süre (saniye)

        EN:
        :param fail_max: Maximum number of failures before opening the circuit (default: 3)
        :param reset_timeout: Time to wait (in seconds) before trying the function again
        """
        self.fail_max = fail_max
        self.reset_timeout = reset_timeout
        self.fail_count = 0
        self.state = 'CLOSED'
        self.opened_at = None

    def call(self, func, *args, **kwargs):
        """
        TR:
        Verilen fonksiyonu çalıştırır. Devre açıksa ve zaman aşımı dolmamışsa hata fırlatır.
        Yarı açık durumdaysa fonksiyon başarılı olursa devre kapanır.
        Fonksiyon başarısız olursa hata sayısı artırılır ve devre gerekiyorsa açılır.

        EN:
        Executes the provided function. If the circuit is open and the timeout hasn't expired, 
        it raises an error. If in half-open state and the function succeeds, the circuit closes.
        On failure, increments the failure count and opens the circuit if the threshold is reached.

        :param func: Callable function to execute
        :param args: Positional arguments for the function
        :param kwargs: Keyword arguments for the function
        :return: Result of the function if executed successfully
        :raises RuntimeError: If the circuit is open and timeout hasn't passed
        :raises Exception: Re-raises the original exception if the function fails
        """
        now = time.time()

        if self.state == 'OPEN':
            if now - self.opened_at > self.reset_timeout:
                self.state = 'HALF_OPEN'
            else:
                raise RuntimeError("Circuit is open")

        try:
            result = func(*args, **kwargs)
            if self.state == 'HALF_OPEN':
                self.state = 'CLOSED'
                self.fail_count = 0
            return result
        except Exception:
            self.fail_count += 1
            if self.fail_count >= self.fail_max:
                self.state = 'OPEN'
                self.opened_at = now
            raise
