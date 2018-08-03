# -*- coding: utf-8 -*-  
"""
IP资源管理模块
        此模块定时从spiderservice获取proxy节点列表
        按照调研的不会被封禁的时间间隔提供给采集系统使用
@author: ZhangFan
@date : 2018/8/1
@e-mail: zhangfan.dz@founder.com
"""
import json
import logging
import thread
import threading
import time
import random


class ResourceManager(threading.Thread):
    """
    IP资源管理模块
        此模块定时从spiderservice获取proxy节点列表
        按照调研的不会被封禁的时间间隔提供给采集系统使用
    """

    class Proxy:
        """
        Proxy 值类
        """

        def __init__(self, ip, time_interval, last_use_time):
            """
            Args：
                ip: ip+端口字符串
                time_interval: ip分配的时间间隔
                last_use_time: 该proxy上次分配时间（当is_free=False时有效)
            """
            self.time_interval = time_interval
            self.last_use_time = last_use_time
            self.ip = ip

        def is_free(self):
            if time.time() - self.last_use_time > self.time_interval:
                return True
            else:
                return False

    def __init__(self, proxy_time_interval, mast_agent):
        """
        Args：
             proxy_time_interval: 每隔ip两次分配的间隔时间
             mast_agent: 获取proxy列表的服务器地址
        """
        super(ResourceManager, self).__init__()
        # 每个ip可使用的时间间隔
        self.proxy_time_interval = proxy_time_interval
        # proxy节点队列
        self.proxy_list = list()
        # 锁
        self.mutex = threading.Lock()
        # 服务器地址
        self.mast_agent = mast_agent

    def run(self):
        """
        定时更新线程
        """
        # 若不采用继承threading类的方式，可采用start_new_thread方法直接调用
        # while True:
        #     thread.start_new_thread(self.refresh,())
        while True:
            self.refresh()

    def refresh(self):
        """
        定时更新可用proxy列表
        思路：
            1.服务器返回的proxy列表中有，self.proxy_list却没有的：加入self.proxy_list
            2.服务器返回的proxy列表中没有，self.proxy_list有的：从self.proxy_list中剔除
        """
        # 获取proxy节点列表
        raw_data = self.get_proxy_list_from_service(self.mast_agent)
        # 解析返回的proxy节点列表
        latest_proxy_list = self.parse_service_ip_list_data(raw_data)

        # 保证线程安全，加锁
        with self.mutex:
            # # TODO 测试，模拟服务器返回的可用proxy变化过程
            # latest_proxy_list = latest_proxy_list[random.randint(0, 7):]

            old_ip_list = list()
            # 更新proxy列表
            for i in range(len(self.proxy_list) - 1, -1, -1):
                old_ip_list.append(self.proxy_list[i].ip)
                if self.proxy_list[i].ip not in latest_proxy_list:
                    logging.debug("删除一个proxy")
                    del self.proxy_list[i]

            for ip in latest_proxy_list:
                if ip not in old_ip_list:
                    # 加入首位
                    tmp = self.Proxy(ip=ip, time_interval=self.proxy_time_interval, last_use_time=time.time())
                    self.proxy_list.insert(0, tmp)
                    logging.debug("加入一个proxy")

            logging.debug("proxy列表更新一次")

        # 每隔一段时间向服务器请求proxy列表，时间可放入配置文件
        time.sleep(5)

    def get_proxy_list_from_service(self, mast_agent):
        """
        向服务器请求proxy列表，不成功时，重试x次
        因为向服务器请求数据的协议未知，此方法暂未实现
        Returns:
                存在可用的ip资源时：返回proxy节点列表json数据
                不存在：None
        """
        string = ''
        logging.debug("已向服务器请求一次proxy节点列表")
        return string

    def get_free_proxy(self):
        """
        获取可用ip资源的接口
        Args：
        Returns:
                存在可用的ip资源时：返回该资源
                不存在：None
        """
        result = None
        # 为保证线程安全，此处加锁

        with self.mutex:
            # 队列不为空检查
            if len(self.proxy_list) > 0:
                # 当队列首节点为可用时
                if self.proxy_list[0].is_free():
                    first_proxy = self.proxy_list.pop(0)
                    result = first_proxy.ip
                    # 设置为已指派，并记录指派时间后加入队列尾
                    first_proxy.last_use_time = time.time()
                    self.proxy_list.append(first_proxy)
            return result

    @staticmethod
    def parse_service_ip_list_data(raw_data):
        """
        解析spiderservice返回的ip列表
        Args：
            raw_data:spiderservice返回ip列表的json数据
        Returns:
                ip端口list
        """
        result = []
        data_dict = json.loads(raw_data)
        if 'proxies' in data_dict and len(data_dict.get('proxies')) > 0:
            for tab in data_dict.get('proxies'):
                if 'addr' in tab:
                    result.append(tab.get('addr'))

        else:
            logging.error("解析spiderservice返回ip列表出错")
            return result
        return result
