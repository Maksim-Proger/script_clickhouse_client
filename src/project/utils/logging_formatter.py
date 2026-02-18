import logging
import sys


def setup_logging(name, level=logging.INFO):
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s")
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)

    # Очищаем старые хендлеры, если они есть у логгера с этим именем
    if logger.handlers:
        logger.handlers.clear()

    logger.addHandler(handler)

    # Чтобы лог не дублировался, если настроен еще и root logger
    logger.propagate = False

    return logger
