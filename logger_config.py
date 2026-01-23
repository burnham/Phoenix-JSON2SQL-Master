import logging
import os

def setup_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    if not logger.handlers:
        sh = logging.StreamHandler(); sh.setFormatter(formatter); logger.addHandler(sh)
        fh = logging.FileHandler('phoenix_debug.log', encoding='utf-8'); fh.setFormatter(formatter); logger.addHandler(fh)
    return logger
