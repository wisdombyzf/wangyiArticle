# -*- encoding:utf-8 -*-
"""
实体库扫表、入库程序；
"""
import copy
import json
import logging
import Queue
import time
import threading

import database_connecter


class EntityTableScanner(object):
    def __init__(self, config):
        self.config = config
        # 站点名称
        self.site_name = config.site_name
        # 扫表时间间隔
        self.refresh_interval = config.entity_refresh_interval
        # 期望得到的数量
        self.expected_size = config.entity_expected_size
        # 数据库链接实例
        self.entity_db_connector = database_connecter.DatabaseConnector(config=config, db_host=config.entity_db_host,
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

    def add_entity(self, identity, object_type, name=None, screen_name=None, auth_token=None, url=None, priority=None,
                   region=None, relation_fetched=None, info_fetched=None, need_fetch_status=None, status_fetched=None):
        temp = dict()
        temp["identity"] = identity
        temp["object_type"] = object_type
        temp["name"] = name
        temp["screen_name"] = screen_name
        temp["auth_token"] = auth_token
        temp["url"] = url
        temp["priority"] = priority
        temp["region"] = region
        temp["relation_fetched"] = relation_fetched
        temp["info_fetched"] = info_fetched
        temp["need_fetch_status"] = need_fetch_status
        temp["status_fetched"] = status_fetched
        try:
            self.entity_queue.put_nowait(temp)
        except Exception as e:
            logging.exception("Insert entity exception, queue size: %d, error: %s" % (self.entity_queue.qsize(), e))
            return False
        return True

    def __insert_entity_to_db(self):
        while not self.__stop:
            try:
                temp = self.entity_queue.get(timeout=5)
            except Exception as e:
                logging.info("No entity need insert to db.")
            else:
                try:
                    ret = self.entity_db_connector.insert_new_entity(identity=temp["identity"],
                                                                     object_type=temp["object_type"],
                                                                     name=temp["name"], screen_name=temp["screen_name"],
                                                                     auth_token=temp["auth_token"], url=temp["url"],
                                                                     priority=temp["priority"], region=temp["region"],
                                                                     relation_fetched=temp["relation_fetched"],
                                                                     info_fetched=temp["info_fetched"],
                                                                     need_fetch_status=temp["need_fetch_status"],
                                                                     status_fetched=temp["status_fetched"])
                    if ret:
                        logging.info("Insert one entity into db, identity: %s" % temp["identity"])
                    else:
                        logging.warning("Insert entity into db failed, msg: %s" % json.dumps(temp, ensure_ascii=False))
                except Exception as e:
                    logging.exception("Insert entity exception: %s" % e)

    def __get_entity_task_from_db(self):
        while not self.__stop:
            try:
                entitys = self.entity_db_connector.get_entity_task(self.expected_size)
            except Exception as e:
                logging.exception("Get task from task table exception: %s" % e)
                entitys = None
            if entitys is not None and isinstance(entitys, list) and len(entitys) > 0:
                tasks = list()
                for temp in entitys:
                    try:
                        object_type = temp.get("object_type", -1)
                        relation_fetched = temp.get("relation_fetched", 1)
                        if relation_fetched == 0:
                            # 保证模块的独立性，未使用ObjectType；如果实体类型有变化，需要修改；
                            # 1：USER，3：GROUP，4：EVENT, 8: SCHOOL, 9: COMPANY
                            if object_type in [1, 3, 4, 8, 9]:
                                relation_task = copy.deepcopy(temp)
                                if object_type == 1:
                                    relation_task["task_type"] = 201
                                else:
                                    relation_task["task_type"] = 204
                                tasks.append(relation_task)
                        info_fetched = temp.get("info_fetched", 1)
                        if info_fetched == 0:
                            info_task = copy.deepcopy(temp)
                            info_task["task_type"] = 101
                            tasks.append(info_task)
                        # 采集推文的任务
                        need_fetch_status = temp.get("need_fetch_status", 0)
                        status_fetched = temp.get("status_fetched", 1)
                        if need_fetch_status == 1 and status_fetched == 0:
                            status_task = copy.deepcopy(temp)
                            status_task["task_type"] = 301
                            status_task["data_range"] = 3600 * 24 * 30
                            tasks.append(status_task)
                    except Exception as e:
                        logging.exception("Get entity format error: %s" % e)
                with self.wait_process_lock:
                    if len(tasks) > 0:
                        self.wait_process_task_list = tasks
            for i in xrange(int(self.refresh_interval / 10)):
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
                        result.append(task)
                        if len(result) >= size:
                            break
                    else:
                        i += 1
            except Exception as e:
                logging.exception(e)
        return result

    def update_entity_info(self, identity, object_type, name=None, screen_name=None, auth_token=None, url=None,
                           priority=None, region=None, relation_fetched=None, info_fetched=None, need_fetch_status=None,
                           status_fetched=None, destroyed=None):
        temp = dict()
        temp["identity"] = identity
        temp["object_type"] = object_type
        temp["name"] = name
        temp["screen_name"] = screen_name
        temp["auth_token"] = auth_token
        temp["url"] = url
        temp["priority"] = priority
        temp["region"] = region
        temp["relation_fetched"] = relation_fetched
        temp["info_fetched"] = info_fetched
        temp["need_fetch_status"] = need_fetch_status
        temp["status_fetched"] = status_fetched
        temp["destroyed"] = destroyed
        try:
            self.update_entity_queue.put_nowait(temp)
            return True
        except Exception as e:
            logging.warning("Entity update queue is full.")
            return False

    def __update_entity_info(self):
        while not self.__stop:
            try:
                try:
                    temp = self.update_entity_queue.get(timeout=5)
                except Exception as e:
                    logging.info("No entity wait to update.")
                    continue
                ret = self.entity_db_connector.update_entity_table(identity=temp["identity"],
                                                                   object_type=temp["object_type"],
                                                                   name=temp["name"], screen_name=temp["screen_name"],
                                                                   auth_token=temp["auth_token"], url=temp["url"],
                                                                   priority=temp["priority"], region=temp["region"],
                                                                   relation_fetched=temp["relation_fetched"],
                                                                   info_fetched=temp["info_fetched"],
                                                                   need_fetch_status=temp["need_fetch_status"],
                                                                   status_fetched=temp["status_fetched"],
                                                                   destroyed=temp["destroyed"])
                if not ret:
                    logging.warning("Update entity failed: %s." % json.dumps(temp))
            except Exception as e:
                logging.exception("Update entity exception: %s" % e)

    def start(self):
        ret = self.entity_db_connector.connect_database()
        while not ret and not self.__stop:
            logging.warning("Can not connect task table.")
            time.sleep(2)
        t = threading.Thread(target=self.__insert_entity_to_db)
        t.start()
        self.threads.append(t)
        t = threading.Thread(target=self.__update_entity_info)
        t.start()
        self.threads.append(t)
        t = threading.Thread(target=self.__get_entity_task_from_db)
        t.start()
        self.threads.append(t)

    def stop(self):
        self.__stop = True
        self.entity_db_connector.close_connect()
        for t in self.threads:
            t.join()
        logging.info("Refresh entity task stop.")



