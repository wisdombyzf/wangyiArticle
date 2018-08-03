# -*- coding: utf-8 -*-  
"""

@File : wangyi_article_ip_test.py

@Author: 张帆

@Date : 2018/8/2

@Desc : 网易云app ip封禁测试

"""
import logging
import random
import thread
import time

import requests


def init():
    """
    读取配置文件，设置日志输出规则等初始化工作
    :return:
    """
    # 将request库的logging等级设为warning，防止其打印很多无用信息
    logging.getLogger("requests").setLevel(logging.WARNING)
    # 输出到日志文件
    logging.basicConfig(filename='log/wangyi_article_list.log',
                        format='[%(asctime)s-%(levelname)s:%(message)s]', level=logging.INFO,
                        filemode='a', datefmt='%Y-%m-%d%I:%M:%S %p')
    console = logging.StreamHandler()
    logging.getLogger('').addHandler(console)


def get_json(url):
    """
    根据url获取对应内容
    :param url: url
    :return: json数据
    """
    http_headers = {
        'User-Agent': 'NewsApp/39.1 Android/8.1.0 (xiaomi/Redmi Note 5)',
        'Connection': 'Keep-Alive',
        'Accept-Encoding': 'gzip',
        'Host': 'c.m.163.com',
    }
    # 最大重试次数
    max_try = 4
    while max_try > 0:
        max_try -= 1
        try:
            response = requests.get(url, headers=http_headers, timeout=(6.05, 10))
            # 成功则跳出循环
            return response.text
        except Exception as e:
            logging.error('download exception: %s ' % e.message)
    if max_try <= 0:
        logging.error("下载重试次数已超过")


def get_article_list():
    """
    文章列表ip封禁测试

    """
    url_data = ['T1442399715369', 'T1438741261740', 'T1445826421160', 'T1439967087307', 'T1446544714197',
                'T1444285958437', 'T1471517065862', 'T1472278361992']
    url = 'http://c.m.163.com/nc/subscribe/list/' + url_data[
        random.randint(0, len(url_data) - 1)] + '/all/0-20.html'
    data = get_json(url)
    logging.info("访问 %s ;返回数据长度 %s", url, len(data))


def get_article():
    url_data = ['DO29PQQP051184MS', 'DO23S0KT051184MS', 'DO1QB4FN051184MS', 'DO1M71TH051184MS', 'DNN9FFKA051184MS',
                'DNN8RTLD051184MS', 'DNJ0LK3G051184MS', 'DNJ03VV1051184MS']
    url = 'http://c.m.163.com/nc/article/preload/' + url_data[
        random.randint(0, len(url_data) - 1)] + '/full.html'
    data = get_json(url)
    logging.info("访问 %s ;返回数据长度 %s", url, len(data))


if __name__ == '__main__':
    init()
    while True:
        thread.start_new_thread(get_article_list, ())
        # 平均0.5秒访问一次
        time.sleep(random.random())
