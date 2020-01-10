# -*- coding: utf-8 -*-
from scrapy import cmdline


# 深圳证券交易所
# cmdline.execute('scrapy crawl shenzhen_stock'.split())
# 上海证券交易所
# cmdline.execute('scrapy crawl shanghai_stock'.split())
# 全国中小企业股份转让系统
# cmdline.execute('scrapy crawl national_stock'.split())

cmdline.execute("scrapy crawlall".split())
# 清空url跑全部任务
# cmdline.execute("scrapy crawlall -a deltafetch_reset=1".split())