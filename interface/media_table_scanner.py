# -*- encoding:utf-8 -*-
"""
自媒体账号列表扫表、入库程序；
"""
import copy
import json
import logging
import Queue
import time
import threading

import database_connecter


class MediaTableScanner(object):
    def __init__(self, config):
        self.config = config
        # 站点名称
        self.site_name = config.site_name
        # 扫表时间间隔
        self.refresh_interval = config.entity_refresh_interval
        # 期望得到的数量
        self.expected_size = config.entity_expected_size
        # 数据库链接实例
        self.media_db_connector = database_connecter.DatabaseConnector(config=config, db_host=config.entity_db_host,
                                                                       db_port=config.entity_db_port,
                                                                       db_name=config.entity_db_name, db_table_name="",
                                                                       db_charset=config.entity_db_charset,
                                                                       db_user=config.entity_db_user,
                                                                       db_pass_word=config.entity_db_pass_word)
        # 等待入库的实体
        self.entity_queue = Queue.Queue(maxsize=10000)
        # 等待采集系统处理的任务
        self.wait_process_task_list = list()
        self.wait_process_lock = threading.Lock()
        # 需要更新数据的任务队列
        self.update_entity_queue = Queue.Queue(maxsize=10000)
        self.threads = list()
        self.__stop = False

    def __get_task_from_db(self):
        """
        定时更新任务：定期更新所有的自媒体号；更新内存中的数据；
        """

    def get_refresh_media_task(self, size=1):
        """
        获取刷新自媒体号获取文章的任务；
        Args:
            size: 期望的任务数量；
        Returns:
            任务列表；
        """
        pass

    def get_refresh_article_task(self, size=1):
        """
        获取刷新自媒体号获取文章增量信息的任务；
        Args:
            size: 期望的任务数量；
        Returns:
            任务列表；
        """
        pass

    def update_media_info(self, identity, task_type):
        """
        更新自媒体上次更新时间信息；
        Args:
            identity: 自媒体账号；
            task_type: 更新类型，0：刷新文章任务；1：刷新增量信息任务；

        Returns:

        """
        pass

    def start(self):
        ret = self.media_db_connector.connect_database()
        while not ret and not self.__stop:
            logging.warning("Can not connect task table.")
            time.sleep(2)
        t = threading.Thread(target=self.__get_task_from_db)
        t.start()
        self.threads.append(t)

    def stop(self):
        self.__stop = True
        self.media_db_connector.close_connect()
        for t in self.threads:
            t.join()
        logging.info("Refresh entity task stop.")
