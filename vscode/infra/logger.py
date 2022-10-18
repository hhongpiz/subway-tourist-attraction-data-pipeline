import logging
from infra.util import cal_std_day

def get_logger(name, before_cnt):
    co_logger = logging.getLogger(name)
    handler = logging.FileHandler('./log/'+cal_std_day(before_cnt)+'.log')
    co_logger.addHandler(handler)
    return co_logger