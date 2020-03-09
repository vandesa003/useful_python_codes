"""
logger

Created On 9th Mar, 2020
Author: bohang.li
"""
import os
import logging
import logging.handlers


def init_logging(log_dir=None, log_file='DenseFNet.log',
                 log_level='info', log_size_mb=10, backup_num=6):
    if log_dir is None:
        log_dir = os.path.abspath("./logs")
    else:
        log_dir = os.path.abspath(log_dir)
    print("create log in {0}...".format(log_dir))
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    logging.basicConfig()
    log = logging.getLogger("Gundam")
    if 'debug' == log_level:
        log.setLevel(logging.DEBUG)
    elif 'info' == log_level:
        log.setLevel(logging.INFO)
    elif 'warning' == log_level:
        log.setLevel(logging.WARNING)
    elif 'error' == log_level:
        log.setLevel(logging.ERROR)
    elif 'critical' == log_level:
        log.setLevel(logging.CRITICAL)
    else:
        log.setLevel(logging.FATAL)

    handler = logging.handlers.RotatingFileHandler(
        log_dir + '/' + log_file, maxBytes=int(log_size_mb) * 1024 * 1024, backupCount=int(backup_num))
    formatter = logging.Formatter(
        '%(levelname)s:%(asctime)s %(message)s', "%Y-%m-%d %H:%M:%S")
    handler.setFormatter(formatter)
    log.addHandler(handler)
    return log
