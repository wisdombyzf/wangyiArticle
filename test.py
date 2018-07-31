# -*- coding: utf-8 -*-
import logging
import requests
import json
import logging
import shelve
import time
import os
import random
import HttpClient

import ConfigParser


# class HttpClient:
#     http_headers = {
#         'Host': 'lf.snssdk.com',
#         'Connection': 'keep-alive',
#         'User-Agent': 'Mozilla/5.0 (Linux; Android 7.1.1; MI MAX 2 Build/NMF26F; wv) AppleWebKit/537.36 (KHTML, like Gecko) Version/4.0 Chrome/64.0.3282.137 Mobile Safari/537.36 JsSdk/2 NewsArticle/6.8.1 NetType/wifi',
#         'Accept-Encoding': 'gzip',
#         'Cookie': 'install_id=38304315933; ttreq=1$aeddd2a1513c86900e2dbda445dfd9bafc66d57d; UM_distinctid=164aca6e2ce20c-013294bf0457ce-5f304712-43113-164aca6e2cf66; alert_coverage=82; qh[360]=1; CNZZDATA1264530760=825533402-1531900092-%7C1532412638; sid_guard=828967e0eda90fd37a4ffd22926dfe3a%7C1532416687%7C21600%7CTue%2C+24-Jul-2018+13%3A18%3A07+GMT; uid_tt=939bd569aea65807b6d3b340e40f52b3; sid_tt=828967e0eda90fd37a4ffd22926dfe3a; sessionid=828967e0eda90fd37a4ffd22926dfe3a; odin_tt=003d7c695b3fdf3871cd651a41cbfa59deeb10922c0cecbad0d43c5b74e21968bbaa253cc886a08d08a854151ec3047d'
#     }
#
#     # 最大重试次数
#     maxTryNum = 4
#
#     def __init__(self):
#         pass
#
#     '''
#     封装requests.get()方法，下载url，如果下载失败则重试，重试四次后放弃
#     Args：
#         url: 需要下载的url
#     Returns:
#         requests.get()得到的response.text
#     '''
#
#     def doget(self, url):
#         """
#             封装requests.get()方法，下载url，如果下载失败则重试，重试四次后放弃
#             Args：
#                 url: 需要下载的url
#             Returns:
#                 requests.get()得到的response.text
#             """
#
#         max_try = 4  # 最大重试次数
#         while max_try > 0:
#             max_try -= 1
#             try:
#                 response = requests.get(url, headers=self.http_headers, timeout=(6.05, 10))
#                 return response.text  # 成功则跳出循环
#             except Exception as e:
#                 logging.error('download exception: %s ' % e.message)
#         if max_try <= 0:
#             logging.error("download  exceed max_try")
#
#     def doPost(self, url, params):
#         max_try = self.maxTryNum
#         while max_try:
#             try:
#                 response = requests.post(url, params, header=self.http_headers, timout=(6.05, 10))
#                 return response
#             except Exception as e:
#                 logging.error('download exception: %s ' % e.message)
#         if max_try <= 0:
#             logging.error("download  exceed max_try")


class WangyiArticleVo(object):
    """
    网易文章属性类，用来储存将要提取的文章属性，如标题，主体，评论数等等
    此处命名直接为json数据中的格式
    """

    def __init__(self, docid, title, body, articleType, ptime, searchKw, threadVote, ename, tname, shareLink, category,
                 replyCount):
        '''
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
        '''
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


def ini():
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




    # url = "http://c.m.163.com/nc/article/preload/" + get_article_id() + "/full.html"
    # data = get_article(url)
    # article = parse_article(data)
# http://c.m.163.com/nc/article/preload/DN5D8FQU051184MS/full.html
