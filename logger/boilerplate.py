import logging


def get_logger(logLevel=logging.DEBUG):
    logging.basicConfig(level=logLevel, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    return logging
