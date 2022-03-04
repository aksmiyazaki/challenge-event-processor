import logging


def get_logger(service_id=None, logLevel=logging.DEBUG):
    log_format = "[%(levelname)7s][%(asctime)s][%(filename)20s:%(lineno)4s:%(funcName)20s()] -  %(message)s"
    if service_id:
        log_format = f"[{service_id}]{log_format}"
    logging.basicConfig(level=logLevel, format=log_format)
    return logging
