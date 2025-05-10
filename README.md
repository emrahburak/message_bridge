# message_bridge

Python tabanlı, mesaj iletimini REST API ya da mesaj kuyrukları (broker) üzerinden gerçekleştiren esnek bir kütüphane.

## Özellikler

- REST API'ye mesaj gönderimi (POST)
- RabbitMQ, Redis gibi broker'lara mesaj yönlendirme desteği
- Circuit Breaker (kesme devresi) ile hata kontrolü
- Builder pattern ile kolay yapılandırma

## Kurulum

```bash
pip install git+https://github.com/emrahburak/message_bridge.git

