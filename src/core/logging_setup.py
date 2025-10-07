import logging, sys, os
from venv import logger

def configure_logger(name: str = "prices_ingestor") -> logging.Logger:
    logger = logging.getLogger(name)
    level = os.getenv("LOG_LEVEL", "INFO").upper()
    logger.setLevel(getattr(logging, level, logging.INFO))


    if not logger.handlers:
        fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(fmt)
        logger.addHandler(ch)


        log_file = os.getenv("LOG_FILE", "prices_ingestor.log")
        try:
            fh = logging.FileHandler(log_file, encoding="utf-8")
            fh.setFormatter(fmt)
            logger.addHandler(fh)
        except Exception:
            # ambiente readonly (Actions): ignora file handler
            pass
    return logger