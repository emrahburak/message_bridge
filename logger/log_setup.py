import logging
from logging.handlers import RotatingFileHandler

def setup_logger():
    """
    TR:
    Uygulama için hem dosyaya hem de konsola log yazacak şekilde yapılandırılmış bir logger kurar.
    - `app.log` dosyasına 5MB'ye kadar log yazar ve 3 yedek tutar (RotatingFileHandler).
    - Aynı loglar eşzamanlı olarak konsola da yazılır (StreamHandler).
    - `redis`, `pika` ve `pymongo` kütüphanelerinin log seviyeleri WARNING olarak ayarlanır
      böylece gereksiz bilgi mesajları engellenir.
    
    Geriye yapılandırılmış ana logger nesnesini döndürür.

    EN:
    Configures a logger that writes logs both to a rotating log file and the console.
    - Laogs are written to `app.log`, rotating at 5MB with up to 3 backup files.
    - Logs are also streamed to the console in real-time.
    - The log levels for `redis`, `pika`, and `pymongo` libraries are set to WARNING
      to suppress verbose output.

    Returns:
        logging.Logger: Configured root logger instance.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[
            RotatingFileHandler("app.log", maxBytes=5 * 1024 * 1024, backupCount=3),
            logging.StreamHandler()
        ]
    )

    # Redis ve diğer kütüphaneler için log seviyesini uyarı olarak ayarla
    logging.getLogger("redis").setLevel(logging.WARNING)
    logging.getLogger("pika").setLevel(logging.WARNING)
    logging.getLogger("pymongo").setLevel(logging.WARNING)

    # Ana logger'ı döndür
    return logging.getLogger(__name__)
