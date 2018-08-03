# -*- encoding:utf-8 -*-

import conf
import logging
import MySQLdb
import threading
import time
import datetime
import hashlib
import log


SITETYPE = {"facebook": 1, "twitter": 2, "linkedin": 3, "googleplus": 4, "instagram": 5}


class DatabaseConnector:
    def __init__(self, config, db_host, db_port, db_name, db_table_name, db_charset, db_user, db_pass_word):
        """
        数据库连接初始化；每连接一个数据库需要实例化一个对象；
        Args:
            config: 配置参数；
            db_host: 数据库ip；
            db_port: 数据库port；
            db_name: 数据库名称；
            db_table_name: 数据库表名称；
            db_charset: 数据库编码；
            db_user: 数据库登录用户名；
            db_pass_word: 登录密码；
        """
        self.config = config
        self.host = db_host
        self.port = db_port
        self.db_name = db_name
        self.table_name = db_table_name
        self.charset = db_charset
        self.user = db_user
        self.pass_word = db_pass_word
        self.connect_lock = threading.Lock()
        self.connect = None

    def connect_database(self):
        """
        连接数据库，初始化后需要执行一次；
        连接断开后，再次执行；异常安全；
        Returns:
            True：连接成功；
            False：连接失败；
        """
        if self.connect is not None:
            try:
                self.connect.close()
            except Exception as e:
                logging.exception("Close database exception: %s" % e)
        try:
            self.connect = MySQLdb.connect(host=self.host, port=self.port, user=self.user,
                                           passwd=self.pass_word, db=self.db_name, charset=self.charset)
            self.connect.autocommit(1)
        except Exception as e:
            logging.exception("Connect database exception: %s" % e)
            return False
        return True

    def close_connect(self):
        if self.connect is not None:
            try:
                self.connect.close()
            except Exception as e:
                logging.exception("Close database exception: %s" % e)

    def get_ontime_tasks(self, table_name, site, size=1000):
        """
        xxs任务表专用；
        从数据库中取得符合条件的的任务；
        Args:
            table_name: 任务表名；
            site: 站点，SITETYPE；
            size: 期望取出的数量；
        Returns:
            返回任务列表或None；
        """
        query_template = "SELECT * FROM %s WHERE priority=%d AND site=%d AND stop=0 AND received_stop=0 AND invalid=0 AND (finish=0 OR task_period=1)"
        result = list()
        temp_result_dict = dict()
        for i in range(1, 10):
            query = query_template % (table_name, i, site)
            logging.debug("Get %d tasks from database, sql: %s" % (size, query))
            try:
                temp = self.__get_ontime_from_database(query=query, table_name=table_name, size=size)
            except Exception as e:
                logging.exception(e)
                continue
            try:
                if isinstance(temp, dict):
                    for key in temp:
                        if key not in temp_result_dict:
                            temp_result_dict[key] = list()
                        one_type_result_list = temp_result_dict[key]
                        for item in temp[key]:
                            if len(one_type_result_list) >= size:
                                break
                            one_type_result_list.append(item)
            except Exception as e:
                logging.exception(e)
        for task_type in temp_result_dict:
            one_type_result_list = temp_result_dict[task_type]
            logging.debug("Get task from task table, task type: %d, size: %d" % (task_type, len(one_type_result_list)))
            insert_index = 0
            for item in one_type_result_list:
                if len(result) == 0:
                    result = one_type_result_list
                    break
                priority = item.get("priority", 10)
                if priority is None:
                    priority = 10
                create_time = item.get("create_time")
                if create_time is None:
                    create_time = datetime.datetime.now()
                while insert_index < len(result):
                    if priority > result[insert_index].get("priority", 10):
                        insert_index += 1
                        continue
                    if priority == result[insert_index].get("priority", 10):
                        other_create_time = result[insert_index].get("create_time")
                        if other_create_time is None:
                            other_create_time = create_time
                        if create_time > other_create_time:
                            insert_index += 1
                            continue
                    result.insert(insert_index, item)
                    break
                if insert_index == len(result):
                    result.append(item)
        logging.debug("Get task from database: %d" % len(result))
        return result

    def __get_ontime_from_database(self, query, table_name, size=1000):
        """
        xxs任务表专用；
        执行sql语句，从数据库获取前size条数据；数据库异常时，重连数据库；
        Args:
            query: 查询sql语句；
            size: 如果查询到很多，取前多少条；
        Returns:
            任务列表或None；
        """
        result = dict()
        with self.connect_lock:
            if self.connect is None:
                self.connect_database()
            try:
                cursor = self.connect.cursor(cursorclass=MySQLdb.cursors.SSDictCursor)
                total_num = cursor.execute(query=query)
                while True:
                    row_data = cursor.fetchone()
                    if not isinstance(row_data, dict):
                        break
                    else:
                        task_period = row_data.get("task_period", 0)
                        task_type = row_data.get("task_type", 0)
                        if task_period == 1:
                            latest_update_time = row_data.get("latest_update_time", 0)
                            if isinstance(latest_update_time, datetime.datetime):
                                # 判断周期任务是否失效
                                if task_type == 601 or task_type == 602:
                                    # 动态评论和动态点赞列表刷新有效期判断，超过有效期，设置为已完成，不再采集；
                                    create_time = row_data.get("create_time", 0)
                                    task_valid_time = row_data.get("task_valid_time", 0)
                                    if isinstance(create_time, datetime.datetime):
                                        create_time = time.mktime(create_time.timetuple())
                                        if create_time + task_valid_time < time.time():
                                            tid = row_data.get("tid", 0)
                                            # self.update_task_finish(table_name=table_name, tid=tid, finish=1)
                                            logging.info("Period task valid time out, tid: %d" % tid)
                                            continue
                                latest_update_time = time.mktime(latest_update_time.timetuple())
                                interval = row_data.get("interval", 0)
                                if latest_update_time + interval > time.time():
                                    logging.debug("Period task not ontime.")
                                    continue
                        if task_type not in result:
                            result[task_type] = list()
                            result[task_type].append(row_data)
                            continue
                        one_type_result_list = result[task_type]
                        j = 0
                        while j < len(one_type_result_list):
                            if row_data.get("create_time", 0) < one_type_result_list[j].get("create_time", 0):
                                one_type_result_list.insert(j, row_data)
                                break
                            j += 1
                        if j == len(one_type_result_list):
                            one_type_result_list.append(row_data)
                        if len(one_type_result_list) > size:
                            one_type_result_list.pop()
                cursor.close()
                self.connect.commit()
            except Exception as e:
                logging.exception("Get data from database exception: %s" % e)
                self.connect_database()
            return result

    def get_stop_task(self, table_name, site, need_size=10000):
        """
        xxs 任务专用；
        从数据库中取得需要停止的任务；
        Args:
            table_name: 任务表名；
            site: 站点，SITETYPE；
            need_size: 期望取出的数量；
        Returns:
            返回任务列表或None；
        """
        query_template = "SELECT * FROM %s WHERE site=%d AND stop=1 AND received_stop=0;"
        query = query_template % (table_name,  site)
        logging.debug("Get stop task from database, sql: %s" % query)
        result = self.__get_data_from_database(query=query, size=need_size)
        if isinstance(result, tuple):
            result = list(result)
        logging.debug("Get stop task from database: %d" % len(result))
        return result

    def __get_data_from_database(self, query, size=100):
        """
        执行sql语句，从数据库获取前size条数据；数据库异常时，重连数据库；
        Args:
            query: 查询sql语句；
            size: 如果查询到很多，取前多少条；
        Returns:
            任务列表或None；
        """
        result = list()
        with self.connect_lock:
            if self.connect is None:
                self.connect_database()
            try:
                cursor = self.connect.cursor(cursorclass=MySQLdb.cursors.SSDictCursor)
                total_num = cursor.execute(query=query)
                # if total_num > 0:
                result = cursor.fetchmany(size=size)
                # logging.debug("Get data from db, size: %d" % len(result))
                cursor.close()
                self.connect.commit()
            except Exception as e:
                logging.exception("Get data from database exception: %s" % e)
                self.connect_database()
        return result

    def update_task_start_time(self, table_name, tid):
        """
        xxs任务表专用；
        更新任务开始处理时间；
        Args:
            tid: 任务id；
            table_name：表名称；
        Returns:
            True：更新成功；
            False：更新失败；
        """
        start_time = time.strftime("%Y-%m-%d %H:%M:%S")
        query = "UPDATE %s SET start_time='%s' WHERE tid=%d" % (table_name, start_time, tid)
        logging.debug("Update query:%s" % query)
        return self.__update_table(query=query)

    def update_task_recive_stop(self, table_name, tid):
        """
        xxs任务表专用；
        更新接收stop信号的标志；
        Args:
            tid: 任务id；
            table_name：表名称；
        Returns:
            True：更新成功；
            False：更新失败；
        """
        query = "UPDATE %s SET received_stop=1 WHERE tid=%d" % (table_name, tid)
        logging.debug("Update query:%s" % query)
        return self.__update_table(query=query)

    def update_task_finish(self, table_name, tid, finish=1):
        """
        xxs任务表专用；
        更新任务完成状态；
        Args:
            tid: 任务id；
            table_name：表名称；
            finish：置finish的值；1：成功，2：失败；
        Returns:
            True：更新成功；
            False：更新失败；
        """
        finish_time = time.strftime("%Y-%m-%d %H:%M:%S")
        query = "UPDATE %s SET finish=%d, finish_time='%s' WHERE tid=%d" % (table_name, finish, finish_time, tid)
        logging.debug("Update query:%s" % query)
        return self.__update_table(query=query)

    def update_invalid_task(self, table_name, tid):
        """
        xxs任务表专用；
        置任务为无效状态；
        Args:
            tid: 任务id；
            table_name：表名称；
        Returns:
            True：更新成功；
            False：更新失败；
        """
        query = "UPDATE %s SET invalid=1 WHERE tid=%d" % (table_name, tid)
        logging.debug("Update query:%s" % query)
        return self.__update_table(query=query)

    def update_latest_update_time_task(self, table_name, tid):
        """
        xxs任务表专用；
        更新任务最近一次处理时间；周期任务使用；
        Args:
            tid: 任务id；
            table_name：表名称；
        Returns:
            True：更新成功；
            False：更新失败；
        """
        latest_update_time = time.strftime("%Y-%m-%d %H:%M:%S")
        query = "UPDATE %s SET latest_update_time='%s' WHERE tid=%d" % (table_name, latest_update_time, tid)
        logging.debug("Update query:%s" % query)
        return self.__update_table(query=query)

    def __update_table(self, query, args=None):
        """
        执行更新语句；数据库异常时，重连数据库；
        Args:
            query: sql语句；
            args: 其它参数；
        Returns:
            True：更新成功；
            False：更新失败；
        """
        with self.connect_lock:
            if self.connect is not None:
                try:
                    cursor = self.connect.cursor()
                    cursor.execute(query=query, args=args)
                    cursor.close()
                    self.connect.commit()
                    return True
                except MySQLdb.Error as e:
                    if e.args[0] == 1062:  # 忽略数据库中重复的的项目
                        logging.info("existed in database %s" % query)
                        return True
                except Exception as e:
                    logging.exception("update database exception % s, error: %s" % (query, e))
            self.connect_database()
            return False

    def insert_new_entity(self, identity, object_type, name=None, screen_name=None, auth_token=None, url=None,
                          priority=None, region=None, relation_fetched=None, info_fetched=None, need_fetch_status=None,
                          status_fetched=None, destroyed=None):
        """
        向实体库加入实体；
        Args:
            identity: 实体id；
            object_type: 实体类型；
            name: 实体名称；
            screen_name: 实体唯一名；
            auth_token: linkedin token；
            url: 实体url；
            priority: 实体优先级；
            region: 实体区域；
            relation_fetched: 关系信息是否已抓取；
            info_fetched：基本信息是否已抓取；
            need_fetch_status：是否需要抓取推文；
            status_fetched：推文是否已抓取；
        Returns:
            成功：True；
            失败：False；
        """
        keys = ["identity", "object_type", "create_time"]
        values = list()
        update_keys = list()
        update_values = list()
        values.append(identity)
        values.append(object_type)
        create_time = time.strftime("%Y-%m-%d %H:%M:%S")
        values.append(create_time)

        if relation_fetched is not None:
            keys.append("relation_fetched")
            values.append(relation_fetched)
            update_keys.append("relation_fetched")
            update_values.append(relation_fetched)

            keys.append("last_fetch_relation_time")
            values.append(create_time)
            update_keys.append("last_fetch_relation_time")
            update_values.append(create_time)

        if info_fetched is not None:
            keys.append("info_fetched")
            values.append(info_fetched)
            update_keys.append("info_fetched")
            update_values.append(info_fetched)

            keys.append("last_fetch_info_time")
            values.append(create_time)
            update_keys.append("last_fetch_info_time")
            update_values.append(create_time)

        if need_fetch_status is not None:
            keys.append("need_fetch_status")
            values.append(need_fetch_status)
            update_keys.append("need_fetch_status")
            update_values.append(need_fetch_status)

        if status_fetched is not None:
            keys.append("status_fetched")
            values.append(status_fetched)
            update_keys.append("status_fetched")
            update_values.append(status_fetched)

            keys.append("last_fetch_status_time")
            values.append(create_time)
            update_keys.append("last_fetch_status_time")
            update_values.append(create_time)

        if destroyed is not None:
            keys.append('destroyed')
            values.append(destroyed)

            update_keys.append("destroyed")
            update_values.append(destroyed)

        if name is not None:
            keys.append("name")
            values.append(name)
            update_keys.append("name")
            update_values.append(name)
        if screen_name is not None:
            keys.append("screen_name")
            values.append(screen_name)
            update_keys.append("screen_name")
            update_values.append(screen_name)
        if auth_token is not None:
            keys.append("auth_token")
            values.append(auth_token)
            update_keys.append("auth_token")
            update_values.append(auth_token)
        if url is not None:
            keys.append("url")
            values.append(url)
            update_keys.append("url")
            update_values.append(url)
        if priority is not None:
            keys.append("priority")
            values.append(priority)
            update_keys.append("priority")
            update_values.append(priority)
        if region is not None:
            keys.append("region")
            values.append(region)
            update_keys.append("region")
            update_values.append(region)

        id_hash = int(hashlib.md5(identity).hexdigest(), 16)
        table_num = abs(id_hash) % self.config.entity_db_table_count + 1
        table_name = self.db_name + "_" + str(table_num)
        with self.connect_lock:
            if self.connect is not None:
                try:
                    sql_query = "INSERT IGNORE INTO %s (%s) VALUES(%s) ON DUPLICATE KEY UPDATE %s" % \
                                (table_name, ",".join(keys), ",".join(["%s"]*len(keys)), "=%s,".join(update_keys)+"=%s")
                    cursor = self.connect.cursor()
                    # logging.debug("Insert or update task: %s" % sql_query)
                    num = cursor.execute(query=sql_query, args=values + update_values)
                    logging.info("Insert or update entity into table: %s, values: %s" % (table_name, keys + values + update_values))
                    cursor.close()
                    self.connect.commit()
                    return True
                except Exception as e:
                    logging.exception("Insert new entity into database exception: %s." % e)
            self.connect_database()
            return False

    def update_entity_table(self, identity, object_type, name=None, screen_name=None, auth_token=None, url=None,
                            priority=None, region=None, relation_fetched=None, info_fetched=None,
                            need_fetch_status=None, status_fetched=None, destroyed=None):
        """
        更新实体数据库；
        Args:
            identity: 实体id；
            object_type: 实体类型；
            name: 实体名称；
            screen_name: 实体唯一名称；
            auth_token: 实体token；
            url: 实体主页url；
            priority: 实体优先级；
            region: 实体区域；
            relation_fetched: 实体关系信息是否已抓取；
            info_fetched: 实体基本信息是否已抓取；
            need_fetch_status: 是否需要采集实体推文；
            status_fetched: 实体推文是否已采集；
            destroyed: 实体是否已注销；
        Returns:
            更新成功：True；
            更新失败：False；
        """
        return self.insert_new_entity(identity=identity, object_type=object_type, name=name, screen_name=screen_name,
                                      auth_token=auth_token, url=url, priority=priority, region=region,
                                      relation_fetched=relation_fetched, info_fetched=info_fetched,
                                      need_fetch_status=need_fetch_status, status_fetched=status_fetched,
                                      destroyed=destroyed)

    """
    def get_entity_task(self, size=1000):
        query_template = "SELECT * FROM %s WHERE priority=%d AND destroyed=0 AND (relation_fetched=0 OR " \
                         "info_fetched=0 OR (need_fetch_status=1 AND status_fetched=0))"
        result = list()
        for i in range(1, 10):
            for j in range(self.config.entity_db_table_count):
                if len(result) >= size:
                    break
                need_size = size - len(result)
                table_num = j + 1
                table_name = self.db_name + "_" + str(table_num)
                query = query_template % (table_name, i)
                # logging.debug("Get data from database, sql: %s" % query)
                temp = self.__get_data_from_database(query=query, size=need_size)
                if isinstance(temp, tuple):
                    temp = list(temp)
                if isinstance(temp, list):
                    for item in temp:
                        item["table"] = table_name
                        result.append(item)
        logging.debug("Get entity task from database: %d" % len(result))
        return result
    """

    def get_entity_task(self, size=1000):
        query_template_list = ["SELECT * FROM %s WHERE priority=%d AND destroyed=0 AND relation_fetched=0",
                               "SELECT * FROM %s WHERE priority=%d AND destroyed=0 AND info_fetched=0",
                               "SELECT * FROM %s WHERE priority=%d AND destroyed=0 AND need_fetch_status=1 "
                               "AND status_fetched=0"]
        result = list()
        for query_template in query_template_list:
            temp_result = list()
            reach_limit = False
            for i in range(1, 10):
                for j in range(self.config.entity_db_table_count):
                    if len(temp_result) >= size:
                        reach_limit = True
                        break
                    need_size = size - len(temp_result)
                    table_num = j + 1
                    table_name = self.db_name + "_" + str(table_num)
                    query = query_template % (table_name, i)
                    # logging.debug("Get data from database, sql: %s" % query)
                    temp = self.__get_data_from_database(query=query, size=need_size)
                    if isinstance(temp, tuple):
                        temp = list(temp)
                    if isinstance(temp, list):
                        # logging.debug("Get entity task: %d" % len(temp))
                        for item in temp:
                            item["table"] = table_name
                            temp_result.append(item)
                    else:
                        logging.error("Result from db format error.")
                if reach_limit:
                    break
            # logging.debug("Get one type entity task: %d." % len(temp_result))
            result.extend(temp_result)
        task_record = dict()
        # logging.debug("Before dup, task size: %d" % len(result))
        if len(result) > 0:
            index = 0
            while index < len(result):
                task = result[index]
                identity = task.get("identity")
                object_type = task.get("object_type")
                if not identity or not object_type:
                    result.pop(index)
                    continue
                if identity in task_record and object_type == task_record[identity]:
                    result.pop(index)
                    continue
                task_record[identity] = object_type
                index += 1
        logging.debug("Get entity task from database: %d" % len(result))
        return result

if __name__ == "__main__":
    log.init_log()
    config = conf.Config()
    print config.__dict__
    # task_table_connector = DatabaseConnector(config=config, db_host=config.task_db_host, db_port=config.task_db_port,
    #                             db_name=config.task_db_name, db_table_name=config.task_db_table_name,
    #                             db_charset=config.task_db_charset, db_user=config.task_db_user,
    #                             db_pass_word=config.task_db_pass_word)
    # task_table_connector.connect_database()
    # result = task_table_connector.get_ontime_tasks(config.task_db_table_name, SITETYPE["twitter"])
    # print result
    # result = task_table_connector.get_stop_task(config.task_db_table_name, SITETYPE["twitter"])
    # print result
    # task_table_connector.update_task_start_time(table_name=config.task_db_table_name, tid=1)
    # task_table_connector.update_task_recive_stop(table_name=config.task_db_table_name, tid=1)
    # task_table_connector.update_task_finish(table_name=config.task_db_table_name, tid=1)
    # task_table_connector.update_invalid_task(table_name=config.task_db_table_name, tid=1)
    # task_table_connector.update_latest_update_time_task(table_name=config.task_db_table_name, tid=1)

    entity_connector = DatabaseConnector(config=config, db_host=config.entity_db_host, db_port=config.entity_db_port,
                                        db_name="twitter", db_table_name="",
                                        db_charset=config.entity_db_charset, db_user=config.entity_db_user,
                                        db_pass_word=config.entity_db_pass_word)
    entity_connector.connect_database()
    print entity_connector.get_entity_task(1)
    # entity_connector.insert_new_entity(identity="796262246103953408", object_type=1, name="新疆共青团",
    #                                   screen_name="xjcylchina", auth_token="", url="https://twitter.com/xjcylchina",
    #                                  priority=6, region="CN")
    # entity_connector.update_entity_table(identity="796262246103953408", object_type=1, name="新疆共青团", screen_name="xjcylchina",
    #                                     auth_token="hello", url="https://twitter.com/xjcylchina",
    #                                     region="CN", relation_fetched=0, info_fetched=1, need_fetch_status=0,
    #                                     destroyed=0)


