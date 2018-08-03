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


class TaskTableScanner(object):
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
        self.task_db_connector = database_connecter.DatabaseConnector(config=config, db_host=config.task_db_host,
                                                                      db_port=config.task_db_port,
                                                                      db_name=config.task_db_name,
                                                                      db_table_name=config.task_db_table_name,
                                                                      db_charset=config.task_db_charset,
                                                                      db_user=config.task_db_user,
                                                                      db_pass_word=config.task_db_pass_word)
        # 等待采集系统处理的任务
        self.wait_process_task_list = list()
        self.wait_process_lock = threading.Lock()
        # 等待更新数据库的队列，存放更新信息{"tid":"123","update_type":1, "finish":2}
        self.wait_update_info_queue = Queue.Queue()
        # 等待停止的任务列表
        self.stop_task_list = list()
        self.stop_task_list_lock = threading.Lock()
        self.threads = list()
        self.__stop = False

    def __get_ontime_task_from_db(self):
        while not self.__stop:
            try:
                tasks = self.task_db_connector.get_ontime_tasks(table_name=self.table_name,
                                                                site=database_connecter.SITETYPE[self.site_name],
                                                                size=self.expected_size)
            except Exception as e:
                logging.exception("Get task from task table exception: %s" % e)
                tasks = None
            if tasks is not None and isinstance(tasks, list) and len(tasks) > 0:
                with self.wait_process_lock:
                    self.wait_process_task_list = tasks
            for i in xrange(self.refresh_interval / 10):
                time.sleep(10)
                if self.__stop:
                    break

    def get_tasks(self, size=1, target_task_type=None, target_object_type=None):
        """
        获取等待采集的任务；
        Args:
            size: 期望的任务数量；
            target_task_type: 期望取得的任务类型, list；
            target_object_type: 目标任务实体类型，list；
        Returns:
            任务列表；
        """
        result = list()
        if target_task_type is None:
            target_task_type = list()
        if target_object_type is None:
            target_object_type = list()
        with self.wait_process_lock:
            try:
                i = 0
                while i < len(self.wait_process_task_list):
                    task_type = self.wait_process_task_list[i].get("task_type", None)
                    object_type = self.wait_process_task_list[i].get("object_type", None)
                    if (len(target_task_type) == 0 or task_type in target_task_type) \
                            and (len(target_object_type) == 0 or object_type in target_object_type):
                        task = self.wait_process_task_list.pop(i)
                        tid = task.get("tid")
                        if tid is not None:
                            self.update_tasks(tid, start_time=True)
                        result.append(task)
                        if len(result) >= size:
                            break
                    else:
                        i += 1
            except Exception as e:
                logging.exception(e)
        return result

    def __get_stop_task_from_db(self):
        while not self.__stop:
            try:
                tasks = self.task_db_connector.get_stop_task(table_name=self.table_name,
                                                             site=database_connecter.SITETYPE[self.site_name],
                                                             need_size=self.expected_size)
            except Exception as e:
                logging.exception("Get stop task from task table exception: %s" % e)
                tasks = None
            if tasks is not None and isinstance(tasks, list):
                with self.stop_task_list_lock:
                    self.stop_task_list = tasks
            for i in xrange(self.refresh_interval * 3 / 100):
                time.sleep(10)
                if self.__stop:
                    break

    def get_stop_tasks(self, size=1):
        """
        获得需要停止的任务；
        Args:
            size: 期望的任务数量；
        Returns:
            返回任务列表；
        """
        result = list()
        with self.stop_task_list_lock:
            if len(self.stop_task_list) > 0:
                result = copy.deepcopy(self.stop_task_list)
        return result

    def update_tasks(self, tid, start_time=False, receive_stop=False, finish=-1, invalid_task=False,
                     latest_update_time=False):
        """
        更新任务数据库；
        Args:
            tid: 任务tid；
            start_time: 是否更新任务开始时间；update_type=1
            receive_stop: 是否更新接收到stop标志; update_type=2
            finish: 是否更新finish标志；update_type=3
            invalid_task: 是否更新任务无效标志；update_type=4
            latest_update_time: 是否更新任务最新刷新时间；update_type=5
        """
        if start_time:
            temp = {"tid": tid, "update_type":1}
            self.wait_update_info_queue.put(temp)
        if receive_stop:
            temp = {"tid": tid, "update_type": 2}
            self.wait_update_info_queue.put(temp)
        if finish != -1:
            temp = {"tid": tid, "update_type": 3, "finish": finish}
            self.wait_update_info_queue.put(temp)
        if invalid_task:
            temp = {"tid": tid, "update_type": 4}
            self.wait_update_info_queue.put(temp)
        if latest_update_time:
            temp = {"tid": tid, "update_type": 5}
            self.wait_update_info_queue.put(temp)

    def __update_task_info(self):
        while not self.__stop:
            try:
                try:
                    temp = self.wait_update_info_queue.get(timeout=5)
                except Exception as e:
                    logging.info("No info wait to update.")
                    continue
                tid = temp.get("tid", None)
                update_type = temp.get("update_type", None)
                if update_type == 1 and tid is not None:
                    self.task_db_connector.update_task_start_time(table_name=self.table_name, tid=tid)
                if update_type == 2 and tid is not None:
                    self.task_db_connector.update_task_recive_stop(table_name=self.table_name, tid=tid)
                if update_type == 3 and tid is not None:
                    finish = temp.get("finish", None)
                    if finish is not None:
                        self.task_db_connector.update_task_finish(table_name=self.table_name, tid=tid, finish=finish)
                if update_type == 4 and tid is not None:
                    self.task_db_connector.update_invalid_task(table_name=self.table_name, tid=tid)
                if update_type == 5 and tid is not None:
                    self.task_db_connector.update_latest_update_time_task(table_name=self.table_name, tid=tid)
            except Exception as e:
                logging.exception("Update task exception: %s" % e)

    def start(self):
        ret = self.task_db_connector.connect_database()
        while not ret and not self.__stop:
            logging.warning("Can not connect task table.")
            time.sleep(2)
        t = threading.Thread(target=self.__get_ontime_task_from_db)
        t.start()
        self.threads.append(t)
        t = threading.Thread(target=self.__get_stop_task_from_db)
        t.start()
        self.threads.append(t)
        t = threading.Thread(target=self.__update_task_info)
        t.start()
        self.threads.append(t)

    def stop(self):
        self.__stop = True
        self.task_db_connector.close_connect()
        for t in self.threads:
            t.join()
        logging.info("Refresh task table stop.")

