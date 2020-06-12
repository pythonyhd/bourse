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
RANDOM_UA_TYPE = "random"

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


# redis代理设置
PROXY_REDIS_HOST = "117.78.35.12"
# PROXY_REDIS_HOST = "192.168.1.30"
PROXY_REDIS_PORT = 6379
PROXY_REDIS_PASSWORD = ""
PROXY_REDIS_DB = 15
PROXY_SET_NAME = "proxies"

# # mysql 数据保存数据库，清洗库
MYSQL_41_HOST = "114.115.128.41"
# MYSQL_41_HOST = "192.168.1.98"
MYSQL_41_PORT = 3306
MYSQL_41_USERNAME = "root"
MYSQL_41_PASSWORD = "mysql@Axinyong123"
MYSQL_41_DB = "wecat_gjqyxx"

# mysql 数据保存数据库，爬虫服务器
MYSQL_98_HOST = "114.115.201.98"
# MYSQL_98_HOST = "192.168.1.96"
# MYSQL_98_HOST = "127.0.0.1"
MYSQL_98_PORT = 3306
MYSQL_98_USERNAME = "root"
MYSQL_98_PASSWORD = "axy#mysql2019"
MYSQL_98_DB = "wecat_gjqyxx"

# 151mysql服务器
MYSQL_151_HOST = "49.4.86.151"
# MYSQL_151_HOST = "192.168.1.54"
MYSQL_151_PORT = 3306
MYSQL_151_USERNAME = "root"
MYSQL_151_PASSWORD = "root"
MYSQL_151_DB = "company_name"

# es采集库配置, 节点2
ES_HOST_NAME = "114.115.154.235:9999"
ES_INDEX_NAME = "cf_index_db"
ES_INDEX_TYPE = "xzcf"

# 清洗ES
QX_ES_HOST = "114.115.129.113:9999"
QX_ES_INDEX_NAME = "cfdata_qx_index_db"
QX_ES_INDEX_TYPE = "cf_data_type"


# JVM设置
jvmPath = r'D:\Programs\Java\jre1.8.0_162\bin\server\jvm.dll'
jclasspath = r'-Djava.class.path=D:\sfp_works\codes\data_clean\jar\data2es_api.jar'
jdependency = r"-Djava.ext.dirs=D:\sfp_works\codes\data_clean\jar\lib"
jclassName = "main.xzcf.fp.Data2EsApi"
