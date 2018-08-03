# -*- coding: utf-8 -*-


import os


class FileUtil:
    """
    文件工具类
    """

    def __init__(self):
        pass

    def saveObjectToJson(self, object, filePath):
        """
            将result对象转成json字符串，写入txt文件，文件名以时间命名
            每分钟创建一个新文件
            Args：
                result: 包含自媒体账号信息的Result类对象
            Returns:
                无返回值
            """
        try:
            logging.info("file_create_time is " + create_time)
            temp_dict = result.__dict__
            media_str = json.dumps(temp_dict, ensure_ascii=False)
            local_time = time.strftime('%Y%m%d%H%M%S', time.localtime())

            if create_time == '':
                with open('json\\' + local_time + '.txt', 'a') as f:
                    f.write(media_str)
                    f.write('\n')
                    logging.info('write to ' + 'json\\' + local_time + '.txt')
                    f.close()
                return local_time
            else:
                if int(local_time) - int(create_time) > 60:
                    with open('json\\' + local_time + '.txt', 'a') as f:
                        f.write(media_str)
                        f.write('\n')
                        logging.info('write to ' + 'json\\' + local_time + '.txt')
                        f.close()
                    return local_time
                else:
                    with open('json\\' + create_time + '.txt', 'a') as f:
                        f.write(media_str)
                        f.write('\n')
                        logging.info('write to ' + 'json\\' + create_time + '.txt')
                        f.close()
                    return create_time
        except Exception as e:
            logging.error("save to file exception: %s" % e.message)
