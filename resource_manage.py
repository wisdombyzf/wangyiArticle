# -*- coding: utf-8 -*-  
"""

@File : resource_manage.py

@Author: 张帆

@Date : 2018/8/1

@Desc :IP资源管理模块
        此模块定时从spiderservice获取proxy节点列表
        按照调研的不会被封禁的时间间隔提供给采集系统使用
"""
import json
import logging
import threading
import time


class ResourceManager(threading.Thread):
    """
    IP资源管理模块
        此模块定时从spiderservice获取proxy节点列表
        按照调研的不会被封禁的时间间隔提供给采集系统使用
    """

    def __init__(self, proxy_time_interval, mast_agent):
        super(ResourceManager, self).__init__()
        # 每个ip可使用的时间间隔
        self.proxy_time_interval = proxy_time_interval
        # 可用的proxy节点队列
        self.free_proxy_list = list()
        # 已经使用的proxy节点队列
        self.used_proxy_list = list()
        # 储存已经使用的proxy节点队列的时间
        self.used_proxy_time_list = list()
        self.mutex = threading.Lock()

    def run(self):
        """
        定时更新可用proxy列表
        """

        while True:
            # 保证线程安全，加锁
            self.mutex.acquire()
            # 获取proxy节点列表
            raw_data = self.get_proxy_from_service()
            # 解析返回的proxy节点列表
            latest_proxy_list = self.parse_service_ip_list_data(raw_data)
            # 更新proxy列表
            self.update_all_proxy_list(latest_proxy_list)

            logging.debug("proxy列表更新一次,可用proxy大小：%s" % len(self.free_proxy_list))
            self.mutex.release()

            # 此处暂时采用轮询的方式，更新已使用的proxy
            # TODO 测试常数待修改
            for i in range(0, 1, 1):
                self.mutex.acquire()
                self.update_free_proxy_list()
                self.mutex.release()

                time.sleep(10)

    def update_free_proxy_list(self):
        # 截取起始下标
        begin_index = 0
        for i in range(0, len(self.used_proxy_time_list)):
            if (time.time() - self.used_proxy_time_list[i]) > self.proxy_time_interval:
                # 二次保证无重复
                if self.used_proxy_list[i] not in self.free_proxy_list:
                    logging.debug("将已使用的proxy加入可用列表")
                    self.free_proxy_list.append(self.used_proxy_list[i])
                    begin_index = i+1

        print("起始下标为")
        print(begin_index)

        self.used_proxy_list = self.used_proxy_list[begin_index:]
        self.used_proxy_time_list = self.used_proxy_time_list[begin_index:]

    def update_all_proxy_list(self, latest_proxy_list):
        """
        更新可用proxy列表,所有proxy列表中的proxy存在下列几种情况

        1.used_proxy_list,latest_proxy_list都存在：该proxy仍可用，不做处理
        2.free_proxy_list,latest_proxy_list都存在：该proxy仍可用，不做处理
        3.used_proxy_list，free_proxy_list都不存在，latest_proxy_list存在：添加入free_proxy_list
        4.used_proxy_list存在，latest_proxy_list不存在:移除used_proxy_list的该proxy
        5.free_proxy_list存在，latest_proxy_list不存在:移除free_proxy_list的该proxy

        Args：
            latest_proxy_list:服务器可用proxy列表
        """
        # 处理前三种情况
        for i in latest_proxy_list:
            if (i in self.used_proxy_list) or (i in self.free_proxy_list):
                pass
            elif (i not in self.used_proxy_list) and (i not in self.free_proxy_list):
                logging.debug("加入新proxy")
                self.free_proxy_list.append(i)
        # 处理情况4,注意更新used_proxy_list时也要同步更新used_proxy_time_list
        for i in range(len(self.used_proxy_list) - 1, -1, -1):
            if self.used_proxy_list[i] not in latest_proxy_list:
                logging.debug("删除proxy")
                del self.used_proxy_list[i]
                del self.used_proxy_time_list[i]

        # 处理情况5
        for i in range(len(self.free_proxy_list) - 1, -1, -1):
            if self.free_proxy_list[i] not in latest_proxy_list:
                logging.debug("删除proxy")
                del self.free_proxy_list.pop[i]

    def get_proxy_from_service(self):
        """

        Returns:
                存在可用的ip资源时：返回proxy节点列表json数据
                不存在：None
        """
        str = '{"proxies":[{"addr":"123.57.11.109:48135","databus_info":{"concurrent":4,"cpu_cores":4,"cpu_usage":40.45291,"disk_size":21136797696,"free_disk_size":6009720832,"free_mem_size":1278849024,"max_concurrent":500,"mem_size":4015218688,"render_capacity":5,"render_size":0,"task_capacity":50,"task_finished_per_second":5,"task_received_per_second":2,"task_size":0,"total_finished_tasks":63785825,"total_received_tasks":63785829,"total_success_tasks":62310199,"version":1},"locale":"internal","parsers_info":[{"parser_type":"News","parser_version":1},{"parser_type":"regionlistextractor","parser_version":1},{"parser_type":"commonextractor","parser_version":1}],"plugins":[{"cmdline":"sh /home/crawler/spiderproxy/pluginrepository/News/1/start.sh ","cpu_time":3798923210,"cpu_usage":102.28775,"disk_size":167445087,"mem_size":17920000,"plugin_md5":"9339423c69caa03ef68c7ab1f6edc3d1","plugin_name":"News","plugin_version":1,"progress_pid":14479,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":684490752},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.phantomjsdownload.PhantomJSDownloadBootstrap conf/config.properties ","cpu_time":25621170,"cpu_usage":3.0503304,"disk_size":1408734333,"mem_size":325488640,"plugin_md5":"2947272be58d2a3537f502962caf681e","plugin_name":"jsdownloader","plugin_version":1,"progress_pid":2689,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3868045312},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:ParallelGCThreads=2 -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.webpageregioncrawler.listextractor.ListExtractorPluginBootstrap conf/config.properties ","cpu_time":39427950,"cpu_usage":0.8134214,"disk_size":739169149,"mem_size":160247808,"plugin_md5":"e585c6ca49c8561ed95e4e6bcbbc9dfc","plugin_name":"regionlistextractor","plugin_version":1,"progress_pid":1428,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3705917440},{"cmdline":"java -Xmx300m -Xmn200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.commonextractor.CommonExtractorPluginBootstrap conf/config.properties ","cpu_time":75040,"cpu_usage":17.886179,"disk_size":859324835,"mem_size":336916480,"plugin_md5":"c6880b500a3b349add75af9f1c9f8170","plugin_name":"commonextractor","plugin_version":1,"progress_pid":13251,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3970502656},{"cmdline":"java -Xmx800m -Xmn500m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.download.DownloadBootstrap conf/config.properties ","cpu_time":138550410,"cpu_usage":6.304016,"disk_size":3195022221,"mem_size":874352640,"plugin_md5":"9c28cd5fb532726f70c65afa8d96422b","plugin_name":"downloader","plugin_version":1,"progress_pid":25475,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":4625330176}],"support_render":1,"unstarted_plugins":[]},{"addr":"123.57.54.26:49565","databus_info":{"concurrent":9,"cpu_cores":4,"cpu_usage":39.73081,"disk_size":21136797696,"free_disk_size":9384345600,"free_mem_size":1186549760,"max_concurrent":500,"mem_size":4015218688,"render_capacity":5,"render_size":0,"task_capacity":20,"task_finished_per_second":17,"task_received_per_second":16,"task_size":0,"total_finished_tasks":41487664,"total_received_tasks":41487673,"total_success_tasks":40516038,"version":1},"locale":"internal","parsers_info":[{"parser_type":"News","parser_version":1},{"parser_type":"regionlistextractor","parser_version":1},{"parser_type":"commonextractor","parser_version":1}],"plugins":[{"cmdline":"./NewsParserPlugin.o ","cpu_time":3083501100,"cpu_usage":110.37302,"disk_size":162102277,"mem_size":15028224,"plugin_md5":"9339423c69caa03ef68c7ab1f6edc3d1","plugin_name":"News","plugin_version":1,"progress_pid":9343,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":684490752},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.phantomjsdownload.PhantomJSDownloadBootstrap conf/config.properties ","cpu_time":41826910,"cpu_usage":3.2703116,"disk_size":1343484520,"mem_size":350703616,"plugin_md5":"2947272be58d2a3537f502962caf681e","plugin_name":"jsdownloader","plugin_version":1,"progress_pid":14870,"queue_left_capacity":199,"status":"ACTIVE","vm_mem_size":3867906048},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:ParallelGCThreads=2 -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.webpageregioncrawler.listextractor.ListExtractorPluginBootstrap conf/config.properties ","cpu_time":30554520,"cpu_usage":1.2263669,"disk_size":534371231,"mem_size":173359104,"plugin_md5":"e585c6ca49c8561ed95e4e6bcbbc9dfc","plugin_name":"regionlistextractor","plugin_version":1,"progress_pid":9368,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3705638912},{"cmdline":"java -Xmx300m -Xmn200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.commonextractor.CommonExtractorPluginBootstrap conf/config.properties ","cpu_time":102430,"cpu_usage":7.971385,"disk_size":1021549957,"mem_size":370417664,"plugin_md5":"c6880b500a3b349add75af9f1c9f8170","plugin_name":"commonextractor","plugin_version":1,"progress_pid":8000,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3972399104},{"cmdline":"java -Xmx800m -Xmn500m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.download.DownloadBootstrap conf/config.properties ","cpu_time":163243910,"cpu_usage":8.375894,"disk_size":1065625375,"mem_size":887791616,"plugin_md5":"9c28cd5fb532726f70c65afa8d96422b","plugin_name":"downloader","plugin_version":1,"progress_pid":1717,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":4594475008}],"support_render":1,"unstarted_plugins":[]},{"addr":"123.56.227.99:38017","databus_info":{"concurrent":15,"cpu_cores":4,"cpu_usage":80.191505,"disk_size":21136797696,"free_disk_size":9659502592,"free_mem_size":887898112,"max_concurrent":500,"mem_size":4015218688,"render_capacity":5,"render_size":4,"task_capacity":20,"task_finished_per_second":12,"task_received_per_second":16,"task_size":0,"total_finished_tasks":238235876,"total_received_tasks":238235888,"total_success_tasks":231951326,"version":1},"locale":"internal","parsers_info":[{"parser_type":"News","parser_version":1},{"parser_type":"regionlistextractor","parser_version":1},{"parser_type":"commonextractor","parser_version":1}],"plugins":[{"cmdline":"sh /home/crawler/spiderproxy/pluginrepository/News/1/start.sh ","cpu_time":3320258580,"cpu_usage":194.26848,"disk_size":189534425,"mem_size":54087680,"plugin_md5":"9339423c69caa03ef68c7ab1f6edc3d1","plugin_name":"News","plugin_version":1,"progress_pid":18912,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":684490752},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.phantomjsdownload.PhantomJSDownloadBootstrap conf/config.properties ","cpu_time":4806610,"cpu_usage":1.8090452,"disk_size":1360158488,"mem_size":346456064,"plugin_md5":"2947272be58d2a3537f502962caf681e","plugin_name":"jsdownloader","plugin_version":1,"progress_pid":30069,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3870801920},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:ParallelGCThreads=2 -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.webpageregioncrawler.listextractor.ListExtractorPluginBootstrap conf/config.properties ","cpu_time":158128940,"cpu_usage":0.40221217,"disk_size":241323027,"mem_size":162942976,"plugin_md5":"e585c6ca49c8561ed95e4e6bcbbc9dfc","plugin_name":"regionlistextractor","plugin_version":1,"progress_pid":26999,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3710767104},{"cmdline":"java -Xmx300m -Xmn200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.commonextractor.CommonExtractorPluginBootstrap conf/config.properties ","cpu_time":85270,"cpu_usage":6.435395,"disk_size":132514013,"mem_size":364617728,"plugin_md5":"c6880b500a3b349add75af9f1c9f8170","plugin_name":"commonextractor","plugin_version":1,"progress_pid":19660,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3975340032},{"cmdline":"java -Xmx800m -Xmn500m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.download.DownloadBootstrap conf/config.properties ","cpu_time":144867500,"cpu_usage":7.035176,"disk_size":3062340672,"mem_size":884359168,"plugin_md5":"9c28cd5fb532726f70c65afa8d96422b","plugin_name":"downloader","plugin_version":1,"progress_pid":22244,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":4637024256}],"support_render":1,"unstarted_plugins":[]},{"addr":"123.56.165.208:36864","databus_info":{"concurrent":18,"cpu_cores":4,"cpu_usage":33.240444,"disk_size":105555197952,"free_disk_size":91359191040,"free_mem_size":878657536,"max_concurrent":500,"mem_size":4014944256,"render_capacity":5,"render_size":2,"task_capacity":20,"task_finished_per_second":7,"task_received_per_second":11,"task_size":0,"total_finished_tasks":63630321,"total_received_tasks":63630315,"total_success_tasks":62179962,"version":1},"locale":"internal","parsers_info":[{"parser_type":"News","parser_version":1},{"parser_type":"regionlistextractor","parser_version":1},{"parser_type":"commonextractor","parser_version":1}],"plugins":[{"cmdline":"./NewsParserPlugin.o ","cpu_time":3666014120,"cpu_usage":64.42953,"disk_size":160677678,"mem_size":35106816,"plugin_md5":"9339423c69caa03ef68c7ab1f6edc3d1","plugin_name":"News","plugin_version":1,"progress_pid":31832,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":684072960},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.phantomjsdownload.PhantomJSDownloadBootstrap conf/config.properties ","cpu_time":1379400,"cpu_usage":3.7170882,"disk_size":1337511295,"mem_size":212353024,"plugin_md5":"2947272be58d2a3537f502962caf681e","plugin_name":"jsdownloader","plugin_version":1,"progress_pid":12980,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3867545600},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:ParallelGCThreads=2 -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.webpageregioncrawler.listextractor.ListExtractorPluginBootstrap conf/config.properties ","cpu_time":40871560,"cpu_usage":1.0319917,"disk_size":705816908,"mem_size":171274240,"plugin_md5":"e585c6ca49c8561ed95e4e6bcbbc9dfc","plugin_name":"regionlistextractor","plugin_version":1,"progress_pid":28081,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3705430016},{"cmdline":"java -Xmx300m -Xmn200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.commonextractor.CommonExtractorPluginBootstrap conf/config.properties ","cpu_time":70660,"cpu_usage":13.422819,"disk_size":5950554855,"mem_size":355540992,"plugin_md5":"c6880b500a3b349add75af9f1c9f8170","plugin_name":"commonextractor","plugin_version":1,"progress_pid":31267,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3972202496},{"cmdline":"java -Xmx800m -Xmn500m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.download.DownloadBootstrap conf/config.properties ","cpu_time":149067610,"cpu_usage":8.053691,"disk_size":2758268909,"mem_size":879382528,"plugin_md5":"9c28cd5fb532726f70c65afa8d96422b","plugin_name":"downloader","plugin_version":1,"progress_pid":12168,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":4614410240}],"support_render":1,"unstarted_plugins":[]},{"addr":"182.92.192.10:42547","databus_info":{"concurrent":14,"cpu_cores":4,"cpu_usage":52.652573,"disk_size":21136797696,"free_disk_size":7804321792,"free_mem_size":1098612736,"max_concurrent":500,"mem_size":4015218688,"render_capacity":5,"render_size":4,"task_capacity":20,"task_finished_per_second":11,"task_received_per_second":17,"task_size":0,"total_finished_tasks":121674430,"total_received_tasks":121674446,"total_success_tasks":119036954,"version":1},"locale":"internal","parsers_info":[{"parser_type":"News","parser_version":1},{"parser_type":"regionlistextractor","parser_version":1},{"parser_type":"commonextractor","parser_version":1}],"plugins":[{"cmdline":"sh /home/crawler/spiderproxy/pluginrepository/News/1/start.sh ","cpu_time":3956806730,"cpu_usage":89.3617,"disk_size":168930703,"mem_size":10891264,"plugin_md5":"9339423c69caa03ef68c7ab1f6edc3d1","plugin_name":"News","plugin_version":1,"progress_pid":4460,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":684490752},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.phantomjsdownload.PhantomJSDownloadBootstrap conf/config.properties ","cpu_time":76677330,"cpu_usage":2.0263424,"disk_size":1408866664,"mem_size":354406400,"plugin_md5":"2947272be58d2a3537f502962caf681e","plugin_name":"jsdownloader","plugin_version":1,"progress_pid":17791,"queue_left_capacity":198,"status":"ACTIVE","vm_mem_size":3868180480},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:ParallelGCThreads=2 -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.webpageregioncrawler.listextractor.ListExtractorPluginBootstrap conf/config.properties ","cpu_time":81772240,"cpu_usage":0.810537,"disk_size":243633249,"mem_size":160493568,"plugin_md5":"e585c6ca49c8561ed95e4e6bcbbc9dfc","plugin_name":"regionlistextractor","plugin_version":1,"progress_pid":30310,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3705917440},{"cmdline":"java -Xmx300m -Xmn200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.commonextractor.CommonExtractorPluginBootstrap conf/config.properties ","cpu_time":98690,"cpu_usage":5.8793716,"disk_size":1787771092,"mem_size":346415104,"plugin_md5":"c6880b500a3b349add75af9f1c9f8170","plugin_name":"commonextractor","plugin_version":1,"progress_pid":20701,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3970498560},{"cmdline":"java -Xmx800m -Xmn500m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.download.DownloadBootstrap conf/config.properties ","cpu_time":155090200,"cpu_usage":6.8930564,"disk_size":3035426393,"mem_size":878006272,"plugin_md5":"9c28cd5fb532726f70c65afa8d96422b","plugin_name":"downloader","plugin_version":1,"progress_pid":389,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":4595499008}],"support_render":1,"unstarted_plugins":[]},{"addr":"123.56.95.158:58279","databus_info":{"concurrent":10,"cpu_cores":4,"cpu_usage":52.474064,"disk_size":21136797696,"free_disk_size":8498208768,"free_mem_size":2264178688,"max_concurrent":500,"mem_size":8251449344,"render_capacity":5,"render_size":2,"task_capacity":20,"task_finished_per_second":15,"task_received_per_second":6,"task_size":0,"total_finished_tasks":117512785,"total_received_tasks":117512797,"total_success_tasks":115037987,"version":1},"locale":"internal","parsers_info":[{"parser_type":"News","parser_version":1},{"parser_type":"regionlistextractor","parser_version":1},{"parser_type":"commonextractor","parser_version":1}],"plugins":[{"cmdline":"./NewsParserPlugin.o ","cpu_time":4137772570,"cpu_usage":130.5499,"disk_size":177909570,"mem_size":24948736,"plugin_md5":"9339423c69caa03ef68c7ab1f6edc3d1","plugin_name":"News","plugin_version":1,"progress_pid":25882,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":684134400},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.phantomjsdownload.PhantomJSDownloadBootstrap conf/config.properties ","cpu_time":11081260,"cpu_usage":3.0565462,"disk_size":1387067463,"mem_size":283119616,"plugin_md5":"2947272be58d2a3537f502962caf681e","plugin_name":"jsdownloader","plugin_version":1,"progress_pid":26238,"queue_left_capacity":199,"status":"ACTIVE","vm_mem_size":3867787264},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:ParallelGCThreads=2 -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.webpageregioncrawler.listextractor.ListExtractorPluginBootstrap conf/config.properties ","cpu_time":73610330,"cpu_usage":0.815079,"disk_size":247269761,"mem_size":168042496,"plugin_md5":"e585c6ca49c8561ed95e4e6bcbbc9dfc","plugin_name":"regionlistextractor","plugin_version":1,"progress_pid":19435,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3705655296},{"cmdline":"java -Xmx300m -Xmn200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.commonextractor.CommonExtractorPluginBootstrap conf/config.properties ","cpu_time":79740,"cpu_usage":4.890474,"disk_size":1236870925,"mem_size":338407424,"plugin_md5":"c6880b500a3b349add75af9f1c9f8170","plugin_name":"commonextractor","plugin_version":1,"progress_pid":23120,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3971887104},{"cmdline":"java -Xmx800m -Xmn500m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.download.DownloadBootstrap conf/config.properties ","cpu_time":138650140,"cpu_usage":6.7244015,"disk_size":2994018501,"mem_size":855552000,"plugin_md5":"9c28cd5fb532726f70c65afa8d96422b","plugin_name":"downloader","plugin_version":1,"progress_pid":4141,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":4625653760}],"support_render":1,"unstarted_plugins":[]},{"addr":"47.52.39.33:50914","databus_info":{"concurrent":7,"cpu_cores":4,"cpu_usage":36.42843,"disk_size":42140499968,"free_disk_size":32650457088,"free_mem_size":3697328128,"max_concurrent":500,"mem_size":8254595072,"render_capacity":5,"render_size":1,"task_capacity":20,"task_finished_per_second":2,"task_received_per_second":2,"task_size":0,"total_finished_tasks":40431023,"total_received_tasks":40431029,"total_success_tasks":39299532,"version":1},"locale":"foreign","parsers_info":[{"parser_type":"News","parser_version":1},{"parser_type":"regionlistextractor","parser_version":1},{"parser_type":"commonextractor","parser_version":1}],"plugins":[{"cmdline":"./NewsParserPlugin.o ","cpu_time":1679164250,"cpu_usage":26.313105,"disk_size":160680959,"mem_size":15761408,"plugin_md5":"9339423c69caa03ef68c7ab1f6edc3d1","plugin_name":"News","plugin_version":1,"progress_pid":2777,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":684072960},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.phantomjsdownload.PhantomJSDownloadBootstrap conf/config.properties ","cpu_time":1767733400,"cpu_usage":2.2448978,"disk_size":977297590,"mem_size":213925888,"plugin_md5":"2947272be58d2a3537f502962caf681e","plugin_name":"jsdownloader","plugin_version":1,"progress_pid":32551,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3875450880},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:ParallelGCThreads=2 -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.webpageregioncrawler.listextractor.ListExtractorPluginBootstrap conf/config.properties ","cpu_time":74745000,"cpu_usage":0.6125574,"disk_size":17075929,"mem_size":163721216,"plugin_md5":"e585c6ca49c8561ed95e4e6bcbbc9dfc","plugin_name":"regionlistextractor","plugin_version":1,"progress_pid":17996,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3635163136},{"cmdline":"java -Xmx300m -Xmn200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.commonextractor.CommonExtractorPluginBootstrap conf/config.properties ","cpu_time":2000650,"cpu_usage":3.6753445,"disk_size":13052770,"mem_size":345653248,"plugin_md5":"c6880b500a3b349add75af9f1c9f8170","plugin_name":"commonextractor","plugin_version":1,"progress_pid":16775,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3968937984},{"cmdline":"java -Xmx800m -Xmn500m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.download.DownloadBootstrap conf/config.properties ","cpu_time":76460350,"cpu_usage":4.696274,"disk_size":1946093575,"mem_size":801746944,"plugin_md5":"9c28cd5fb532726f70c65afa8d96422b","plugin_name":"downloader","plugin_version":1,"progress_pid":24738,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":4552237056}],"support_render":1,"unstarted_plugins":[]},{"addr":"123.57.55.119:35556","databus_info":{"concurrent":8,"cpu_cores":4,"cpu_usage":36.127743,"disk_size":105555197952,"free_disk_size":96320221184,"free_mem_size":1052213248,"max_concurrent":500,"mem_size":4014944256,"render_capacity":5,"render_size":0,"task_capacity":19,"task_finished_per_second":14,"task_received_per_second":16,"task_size":0,"total_finished_tasks":235331195,"total_received_tasks":235331202,"total_success_tasks":228728515,"version":1},"locale":"internal","parsers_info":[{"parser_type":"News","parser_version":1},{"parser_type":"regionlistextractor","parser_version":1},{"parser_type":"commonextractor","parser_version":1}],"plugins":[{"cmdline":"./NewsParserPlugin.o ","cpu_time":3046187050,"cpu_usage":108.34192,"disk_size":166520534,"mem_size":53661696,"plugin_md5":"9339423c69caa03ef68c7ab1f6edc3d1","plugin_name":"News","plugin_version":1,"progress_pid":21011,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":684072960},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.phantomjsdownload.PhantomJSDownloadBootstrap conf/config.properties ","cpu_time":182769770,"cpu_usage":2.883625,"disk_size":1382151059,"mem_size":323444736,"plugin_md5":"2947272be58d2a3537f502962caf681e","plugin_name":"jsdownloader","plugin_version":1,"progress_pid":10956,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3869806592},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:ParallelGCThreads=2 -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.webpageregioncrawler.listextractor.ListExtractorPluginBootstrap conf/config.properties ","cpu_time":156477350,"cpu_usage":0.82431734,"disk_size":56841959,"mem_size":160423936,"plugin_md5":"e585c6ca49c8561ed95e4e6bcbbc9dfc","plugin_name":"regionlistextractor","plugin_version":1,"progress_pid":10244,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3707527168},{"cmdline":"java -Xmx300m -Xmn200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.commonextractor.CommonExtractorPluginBootstrap conf/config.properties ","cpu_time":82480,"cpu_usage":6.597938,"disk_size":241480359,"mem_size":369090560,"plugin_md5":"c6880b500a3b349add75af9f1c9f8170","plugin_name":"commonextractor","plugin_version":1,"progress_pid":32683,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3972202496},{"cmdline":"java -Xmx800m -Xmn500m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.download.DownloadBootstrap conf/config.properties ","cpu_time":138375630,"cpu_usage":7.0066977,"disk_size":3407083130,"mem_size":884461568,"plugin_md5":"9c28cd5fb532726f70c65afa8d96422b","plugin_name":"downloader","plugin_version":1,"progress_pid":16048,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":4616101888}],"support_render":1,"unstarted_plugins":[]},{"addr":"182.92.82.243:39962","databus_info":{"concurrent":13,"cpu_cores":4,"cpu_usage":48.502995,"disk_size":105555197952,"free_disk_size":92823539712,"free_mem_size":1342722048,"max_concurrent":500,"mem_size":4014944256,"render_capacity":5,"render_size":5,"task_capacity":20,"task_finished_per_second":12,"task_received_per_second":3,"task_size":0,"total_finished_tasks":116978035,"total_received_tasks":116978051,"total_success_tasks":114344297,"version":1},"locale":"internal","parsers_info":[{"parser_type":"News","parser_version":1},{"parser_type":"regionlistextractor","parser_version":1},{"parser_type":"commonextractor","parser_version":1}],"plugins":[{"cmdline":"./NewsParserPlugin.o ","cpu_time":3918146880,"cpu_usage":78.12818,"disk_size":160216381,"mem_size":13430784,"plugin_md5":"9339423c69caa03ef68c7ab1f6edc3d1","plugin_name":"News","plugin_version":1,"progress_pid":32058,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":684072960},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.phantomjsdownload.PhantomJSDownloadBootstrap conf/config.properties ","cpu_time":204560400,"cpu_usage":2.238047,"disk_size":1361892661,"mem_size":302641152,"plugin_md5":"2947272be58d2a3537f502962caf681e","plugin_name":"jsdownloader","plugin_version":1,"progress_pid":26001,"queue_left_capacity":198,"status":"ACTIVE","vm_mem_size":3869790208},{"cmdline":"java -Xmx200m -XX:+UseParallelOldGC -XX:ParallelGCThreads=2 -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.webpageregioncrawler.listextractor.ListExtractorPluginBootstrap conf/config.properties ","cpu_time":76297440,"cpu_usage":0.610687,"disk_size":586551404,"mem_size":173568000,"plugin_md5":"e585c6ca49c8561ed95e4e6bcbbc9dfc","plugin_name":"regionlistextractor","plugin_version":1,"progress_pid":27722,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3705430016},{"cmdline":"java -Xmx300m -Xmn200m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.commonextractor.CommonExtractorPluginBootstrap conf/config.properties ","cpu_time":70970,"cpu_usage":6.5139947,"disk_size":4189715121,"mem_size":332275712,"plugin_md5":"c6880b500a3b349add75af9f1c9f8170","plugin_name":"commonextractor","plugin_version":1,"progress_pid":19897,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":3969175552},{"cmdline":"java -Xmx800m -Xmn500m -XX:+UseParallelOldGC -XX:+ExitOnOutOfMemoryError -Dlog4j.configuration=file:./conf/log4j.properties -cp .:* com.founder.spiderproxy.download.DownloadBootstrap conf/config.properties ","cpu_time":136341800,"cpu_usage":5.6997457,"disk_size":2565150159,"mem_size":883097600,"plugin_md5":"9c28cd5fb532726f70c65afa8d96422b","plugin_name":"downloader","plugin_version":1,"progress_pid":29298,"queue_left_capacity":200,"status":"ACTIVE","vm_mem_size":4593086464}],"support_render":1,"unstarted_plugins":[]}]}'
        logging.debug("已向服务器请求一次proxy节点列表")
        return str

    def get_free_proxy(self):
        """
        获取可用ip资源的接口
        Args：
        Returns:
                存在可用的ip资源时：返回该资源
                不存在：None
        """
        proxy = None
        # 为保证线程安全，此处加锁
        self.mutex.acquire()
        if len(self.free_proxy_list) > 0:
            proxy = self.free_proxy_list.pop(0)
            # 加入已使用队列
            self.used_proxy_list.append(proxy)
            # 同步更新时间队列
            self.used_proxy_time_list.append(time.time())
        print("可用数")
        print(len(self.free_proxy_list))
        print("已用数")
        print(len(self.used_proxy_list))

        self.mutex.release()
        return proxy

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
