# -*- coding: utf-8 -*-  
"""

@File : spider.py

@Author: 张帆

@Date : 2018/8/2

@Desc :

"""
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


class WangyiArticleVo(object):
    """
    网易文章属性类，用来储存将要提取的文章属性，如标题，主体，评论数等等
    此处命名直接为json数据中的格式
    """

    def __init__(self, docid, title, body, articleType, ptime, searchKw, threadVote, ename, tname, shareLink, category,
                 replyCount, url):
        """
        :param docid:文章id
        :param title:文章标题
        :param body:文章内容
        :param articleType:文章类型
        :param ptime:文章发表时间
        :param searchKw:搜索关键词
        :param threadVote:喜欢这篇文章的人数
        :param ename:文章所属网易号的id
        :param tname:网易号的名称
        :param shareLink:分享链接的地址
        :param category:文章内容分类
        :param replyCount:评论数(跟帖数)
        :param url:文章url
        """

        self.url = url
        self.replyCount = replyCount
        self.category = category
        self.shareLink = shareLink
        self.tname = tname
        self.ename = ename
        self.threadVote = threadVote
        self.searchKw = searchKw
        self.ptime = ptime
        self.articleType = articleType
        self.body = body
        self.title = title
        self.docid = docid


def get_article(url):
    """
    获取网易新闻的文章内容
    :param url: 文章url
    :return: 文章内容
    """
    http_headers = {
        'User-Agent': 'NewsApp/39.1 Android/8.1.0 (xiaomi/Redmi Note 5)',
        'Connection': 'Keep-Alive',
        'Accept-Encoding': 'gzip',
        'Host': 'c.m.163.com',
    }
    # TODO 最大重试次数，超时时间等常量待写入配置文件
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


def get_article_list(weMediaID, last_refresh_time):
    """
    获取文章列表
    Args：
        weMediaID：自媒体id
        last_refresh_time:该自媒体上次刷新时间
    Returns：
        该自媒体的文章列表
    """
    article_list = ["DN5D8FQU051184MS"]
    return article_list


def parse_article(text):
    """

    Args：
        text:json字符串
    Returns：
        网易文章对象
    """
    result = None
    try:
        text_dict = json.loads(text)
        data_dict = text_dict.get('DN5D8FQU051184MS')
        title = data_dict.get("title")
        replyCount = data_dict.get("replyCount")
        category = data_dict.get("category")
        shareLink = data_dict.get("shareLink")
        tname = data_dict.get("tname")
        ename = data_dict.get("ename")
        threadVote = data_dict.get("threadVote")
        searchKw = data_dict.get("searchKw")
        ptime = data_dict.get("ptime")
        articleType = data_dict.get("articleType")
        body = data_dict.get("body")
        title = data_dict.get("title")
        docid = data_dict.get("docid")
        result = WangyiArticleVo(docid, title, body, articleType, ptime, searchKw, threadVote, ename, tname, shareLink,
                                 category,
                                 replyCount)
        return result
    except Exception as e:
        logging.error("解析文件错误 %s " % e.message)


def get_article_id():
    """
    获取文章id
    :return:
    """
    return "DN5D8FQU051184MS"


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


def filter():
    """
    本地文章去重
    :return:
    """
    return True


if __name__ == '__main__':
    init()
    manager = resource_manage2.ResourceManager(10, 1)
    manager.start()
    while True:
        print(manager.get_free_proxy())
        time.sleep(0.5)

    # url = "http://c.m.163.com/nc/article/preload/" + get_article_id() + "/full.html"
# data = get_article(url)
# article = parse_article(data)
# http://c.m.163.com/nc/article/preload/DN5D8FQU051184MS/full.html
