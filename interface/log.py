#! -*- encoding:utf-8 -*-
"""
@date：2018-04-08
@author：yangfei
@e-mail：yangfei.dz@founder.com.cn
"""


import logging
from logging.handlers import RotatingFileHandler
import os

#日志初始化
def init_log():
    # 创建日志文件文件夹
    if not os.path.exists("./log"):
        os.makedirs("./log")
    # 设置默认级别
    logging.getLogger('').setLevel(logging.DEBUG)
    # 设置标准输出显示
    console = logging.StreamHandler()
    # console.setLevel(logging.INFO)
    formatter = logging.Formatter('%(levelname)s %(asctime)s %(thread)d %(filename)s[line:%(lineno)d]: %(message)s')
    console.setFormatter(formatter)
    logging.getLogger('').addHandler(console)
    # 设置回滚文件
    RotatingFile = RotatingFileHandler('./log/meatsearch_proxy.log', maxBytes=20 * 1024 * 1024, backupCount=10)
    # RotatingFile.setLevel(logging.INFO)
    # formatter = logging.Formatter('%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
    RotatingFile.setFormatter(formatter)
    logging.getLogger('').addHandler(RotatingFile)
