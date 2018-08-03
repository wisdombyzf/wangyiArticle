# -*- coding: utf-8 -*-
import Queue
import logging
import requests
import json
import logging
import shelve
import time
import os
import random
import http_client
import resource_manage2
import threading
import ConfigParser

import resource_manage3


def init():
    """
    读取配置文件，设置日志输出规则等初始化工作
    :return:
    """
    # 输出到日志文件
    logging.basicConfig(filename='log/debug.log',
                        format='[%(asctime)s-%(filename)s-%(levelname)s:%(message)s]', level=logging.DEBUG,
                        filemode='a', datefmt='%Y-%m-%d%I:%M:%S %p')
    # 读取配置文件
    cf = ConfigParser.ConfigParser()
    cf.read('config/test.conf')
    print cf.get('db', 'db_pass')


if __name__ == '__main__':
    init()
    manager = resource_manage3.ResourceManager(5, 1)
    manager.start()
    while True:
        print(manager.get_free_proxy())
        time.sleep(0.5)

    # url = "http://c.m.163.com/nc/article/preload/" + get_article_id() + "/full.html"
# data = get_article(url)
# article = parse_article(data)
# http://c.m.163.com/nc/article/preload/DN5D8FQU051184MS/full.html
