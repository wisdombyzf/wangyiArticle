# -*- coding: utf-8 -*-

import requests
import logging


class HttpClient:
    # 最大重试次数
    maxTryNum = 4

    def __init__(self):
        pass

    '''
    封装requests.get()方法，下载url，如果下载失败则重试，重试四次后放弃
    Args：
        url: 需要下载的url
    Returns:
        requests.get()得到的response.text
    '''

    def doget(self, url):
        """
            封装requests.get()方法，下载url，如果下载失败则重试，重试四次后放弃
            Args：
                url: 需要下载的url
            Returns:
                requests.get()得到的response.text
        """

        http_headers = {
            'Connection': 'keep-alive',
            'User-Agent': 'Mozilla/5.0 (Linux; Android 7.1.1; MI MAX 2 Build/NMF26F; wv) AppleWebKit/537.36 (KHTML, '
                          'like Gecko) Version/4.0 Chrome/64.0.3282.137 Mobile Safari/537.36 JsSdk/2 NewsArticle/6.8.1 '
                          'NetType/wifi',
            'Accept-Encoding': 'gzip'
        }
        max_try = 4  # 最大重试次数
        while max_try > 0:
            max_try -= 1
            try:
                response = requests.get(url, header=http_headers, timeout=(6.05, 10))
                return response.text  # 成功则跳出循环
            except Exception as e:
                logging.error('download exception: %s ' % e.message)
        if max_try <= 0:
            logging.error("download  exceed max_try")

    def doPost(self, url, params):
        max_try = self.maxTryNum
        while max_try:
            try:
                response = requests.post(url, params, header=self.http_headers, timout=(6.05, 10))
                return response
            except Exception as e:
                logging.error('download exception: %s ' % e.message)
        if max_try <= 0:
            logging.error("download  exceed max_try")
