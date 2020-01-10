# -*- coding: utf-8 -*-
import os

BOT_NAME = 'bourse'

SPIDER_MODULES = ['bourse.spiders']
NEWSPIDER_MODULE = 'bourse.spiders'

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
scrapy基本配置
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
ROBOTSTXT_OBEY = False
LOG_LEVEL = 'INFO'
COMMANDS_MODULE = 'bourse.commands'

project_path = os.path.abspath(os.path.dirname(os.path.dirname(__file__)))
# 文件存储
FILES_STORE = os.path.join(project_path, 'files')  # 存储路径
FILES_EXPIRES = 90  # 失效时间

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
数据存储 相关配置
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
ES_HOST = '49.4.22.216'
ES_PORT = 9999
ES_USERNAME = ''
ES_PASSWORD = ''
INDEX_NAME = 'cf_index_db'
INDEX_TYPE = 'xzcf'

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
redis 相关配置
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
# 缓存去重配置
# REDIS_HOST = 'localhost'
REDIS_HOST = '114.115.201.98'
REDIS_PORT = 6379
REDIS_PASSWORD = 'axy@2019'
REDIS_DB = 3
REDIS_PARAMS = {
    "password": "axy@2019",
    "db": 3,
}

"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
scrapy 请求头
"""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""""
RANDOM_UA_TYPE = "random"