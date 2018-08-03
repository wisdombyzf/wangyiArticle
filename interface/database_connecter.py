# -*- encoding:utf-8 -*-
"""
数据库连接、查找、更新方法；
@author：yangfei
@date：2018.08.01
"""

import conf
import logging
import MySQLdb
import threading
import time
import datetime
import hashlib
import log


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

    def get_ontime_medias(self, size=1000):
        """
        从数据库中取得到达更新时间的自媒体账号，期望获取size个；
        Args:
            size: 期望取出的数量；
        Returns:
            返回任务列表或None；
        """
        pass

    def get_ontime_articles(self, sql_str, size=1000):
        """
        从数据库获取需要更新增量信息的文章列表，如果入口是自媒体账号，则返回自媒体账号列表；
        Args:
            size: 期望得到的数量；
        Returns:
            返回任务列表或None；
        """
        pass

    def get_all_medias(self):
        """
        从数据库获取所有的有效自媒体账号列表；
        Returns:
            返回任务列表或None；
        """
        pass

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

    def update_latest_update_time_tasks(self, identitys):
        """
        更新任务最近一次处理时间；周期任务使用；批量处理；
        Args:
            identity: 自媒体账号id列表或文章id列表；
        Returns:
            True：更新成功；
            False：更新失败；
        """
        pass
        query = "update..."
        # return self.__update_table(query=query)

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


if __name__ == "__main__":
    log.init_log()
    config = conf.Config()
    print config.__dict__

    entity_connector = DatabaseConnector(config=config, db_host=config.entity_db_host, db_port=config.entity_db_port,
                                         db_name="twitter", db_table_name="",
                                         db_charset=config.entity_db_charset, db_user=config.entity_db_user,
                                         db_pass_word=config.entity_db_pass_word)
    entity_connector.connect_database()
    print entity_connector.get_ontime_articles()
