#! -*- encoding:utf-8 -*-
"""
@date：2018-04-08
@author：yangfei
@e-mail：yangfei.dz@founder.com.cn
"""
import ConfigParser
import os.path


class Config(object):
    """
    配置文件参数管理类
    """

    def __init__(self, config_file_path='task_scanner.ini'):
        cp = ConfigParser.SafeConfigParser()
        cp.read(config_file_path)

        # 任务数据库相关
        self.task_db_host = cp.get("TaskTable", "host")
        self.task_db_port = cp.getint("TaskTable", "port")
        self.task_db_name = cp.get("TaskTable", "dbname")
        self.task_db_table_name = cp.get("TaskTable", "table_name")
        self.task_db_charset = cp.get("TaskTable", "charset")
        self.task_db_user = cp.get("TaskTable", "user")
        self.task_db_pass_word = cp.get("TaskTable", "pass_word")

        self.refresh_interval = cp.getint("TaskTable", "task_refresh_interval")
        self.expected_size = cp.getint("TaskTable", "task_expected_size")

        # 实体库相关
        self.entity_db_host = cp.get("EntityTable", "host")
        self.entity_db_port = cp.getint("EntityTable", "port")
        self.entity_db_name = cp.get("EntityTable", "dbname")
        self.entity_db_table_name = cp.get("EntityTable", "table_name")
        self.entity_db_charset = cp.get("EntityTable", "charset")
        self.entity_db_user = cp.get("EntityTable", "user")
        self.entity_db_pass_word = cp.get("EntityTable", "pass_word")
        self.entity_db_table_count = cp.getint("EntityTable", "table_count")

        self.entity_refresh_interval = cp.getint("EntityTable", "entity_refresh_interval")
        self.entity_expected_size = cp.getint("EntityTable", "entity_expected_size")

        # 站点
        self.site_name = cp.get("Site", "site_name")
        # article__task_list缓存队列长度
        self.article_task_list_max_length = cp.get("Zf", "article_task_list_max_length")


if __name__ == "__main__":
    config = Config()
    print config.__dict__
