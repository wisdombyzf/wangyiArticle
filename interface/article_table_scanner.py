# -*- encoding:utf-8 -*-
"""
xxs 任务表扫表、更新表函数封装；
"""
import copy
import logging
import Queue
import time
import threading

import database_connecter


class ArticleTableScanner(object):
    def __init__(self, config):
        self.config = config
        # 站点名称
        self.site_name = config.site_name
        # 任务表名称
        self.table_name = config.task_db_table_name
        # 扫表时间间隔
        self.refresh_interval = config.refresh_interval
        # 期望得到的数量
        self.expected_size = config.expected_size
        # 数据库链接实例
        self.article_db_connector = database_connecter.DatabaseConnector(config=config, db_host=config.task_db_host,
                                                                         db_port=config.task_db_port,
                                                                         db_name=config.task_db_name,
                                                                         db_table_name=config.task_db_table_name,
                                                                         db_charset=config.task_db_charset,
                                                                         db_user=config.task_db_user,
                                                                         db_pass_word=config.task_db_pass_word)
        # 等待采集系统处理的任务
        self.wait_process_task_list = list()
        # 缓存队列长度,默认为1000,可放入配置文件
        self.article_task_list_max_length = config.article_task_list_max_length
        # 锁
        self.wait_process_lock = threading.Lock()

        # 等待更新数据库的队列，存放更新信息{"tid":"123","update_type":1, "finish":2}
        self.wait_update_info_queue = Queue.Queue()
        # 等待停止的任务列表
        self.stop_task_list = list()
        self.stop_task_list_lock = threading.Lock()
        self.threads = list()
        self.__stop = False

    def __get_ontime_task_from_db(self):
        """
        从数据库获取需要刷新增量信息的文章；结果放入队列，提供给采集调度使用；
        优先级在此处体现
        """
        # sql模板,未提高效率，site属性其实可去掉
        query_template = "SELECT * FROM %s WHERE priority=%d and valid=1 AND site=%s"
        list_old_len = 0
        with self.wait_process_lock:
            # 任务队列长度
            list_old_len = len(self.wait_process_task_list)
            # 此次要从数据库取的数据总数
            fetch_all_num = self.article_task_list_max_length - list_old_len
            # 从数据库按优先级依次取数据
            for i in range(1, 6, 1):
                query = query_template % (self.table_name, i, self.site_name)
                try:
                    fetch_num = fetch_all_num / 2
                    # 当取到最后一个优先级时
                    if i == 5:
                        fetch_num = fetch_all_num

                    data = self.article_db_connector.get_ontime_articles(query, size=fetch_num)
                    fetch_all_num = fetch_all_num - len(data)
                    self.wait_process_task_list = self.wait_process_task_list + data
                except Exception as e:
                    logging.exception(e)
                    continue
        '''
        对此次取出的数据量做判定
        防止当数据库无数据（或返回数据很少）时,程序短时间里大量访问数据库
        规则：
            该线程会sleep：（任务队列最大长度/10）/(此次取的数据量+1) 秒
            当最大长度为1000时， sleep范围为：[0.1,100]秒
            达到自适应调节访问数据库频率的目的
        '''
        time.sleep(len(self.article_task_list_max_length / 10) / (list_old_len + 1))

    def get_tasks(self, size=1):
        """
        获取等待采集的任务；
        Args:
            size: 期望的任务数量；
        Returns:
            wait_process_task_list中任务数>期望的任务数量：大小为请求数的list
            wait_process_task_list中任务数<期望的任务数量: wait_process_task_list中所有任务
        """
        with self.wait_process_lock:
            if len(self.wait_process_task_list) < size:
                result = self.wait_process_task_list
            else:
                result = self.wait_process_task_list[0:size]
        return result

    def update_task(self, identity):
        """
        更新任务数据库；
        Args:
            identity: 文章id；
        """
        pass

    def __update_tasks_to_db(self, identitys):
        """
        批量更新；
        Args:
            identitys:

        Returns:

        """
        pass

    def start(self):
        ret = self.article_db_connector.connect_database()
        while not ret and not self.__stop:
            logging.warning("Can not connect task table.")
            time.sleep(2)
        t = threading.Thread(target=self.__get_ontime_task_from_db)
        t.start()
        self.threads.append(t)

    def stop(self):
        self.__stop = True
        self.article_db_connector.close_connect()
        for t in self.threads:
            t.join()
        logging.info("Refresh task table stop.")
