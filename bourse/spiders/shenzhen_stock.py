# -*- coding: utf-8 -*-
import json
import re
from functools import reduce
from io import BytesIO
from urllib.parse import urljoin

import jsonpath
import scrapy
import logging
from pdfminer.pdfparser import PDFParser, PDFDocument, PDFSyntaxError
from pdfminer.pdfinterp import PDFTextExtractionNotAllowed
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBox

logger = logging.getLogger(__name__)


class SzJgcsJlcfSpider(scrapy.Spider):
    name = 'shenzhen_stock'
    allowed_domains = ['szse.cn']

    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'bourse.middlewares.RandomUserAgentMiddleware': 120
        },
        'ITEM_PIPELINES': {
            'bourse.pipelines.BoursePipeline': 300,
            'bourse.pipelines.DownloadFilesPipeline': 330,
            'bourse.pipelines.Save2eEsPipeline': 360,
        },

        "SCHEDULER": "scrapy_redis.scheduler.Scheduler",
        "DUPEFILTER_CLASS": "scrapy_redis.dupefilter.RFPDupeFilter",
        "SCHEDULER_QUEUE_CLASS": "scrapy_redis.queue.SpiderPriorityQueue",
        "SCHEDULER_PERSIST": True,

        "REDIRECT_ENABLED": False,
        "RETRY_ENABLED": True,
        "RETRY_TIMES": 9,
        "DOWNLOAD_TIMEOUT": 25,
    }

    def start_requests(self):
        """
        入口
        :return:
        """
        start_urls = [
            # 监管措施
            'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1800_jgxxgk&TABKEY=tab1&PAGENO=1&selectBkmc=0',
            # 纪律处分
            'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1800_jgxxgk&TABKEY=tab2&PAGENO=1&selectGsbk=0',
            # 问询函件-主板
            'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=main_wxhj&TABKEY=tab1&PAGENO=1',
            # 问询函件-中小企业版
            'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=main_wxhj&TABKEY=tab2&PAGENO=1',
            # 问询函件-创业板
            'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=main_wxhj&TABKEY=tab3&PAGENO=1',
            # 上市公司诚信档案-处罚与处分记录
            'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1759_cxda&TABKEY=tab1&PAGENO=1',
            # 上市公司诚信档案-中介机构处罚与处分信息
            'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1903_detail&loading=first',
            # 债券信息-问询函
            'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=ZQ_WXHJ&TABKEY=tab1&PAGENO=1',
            # 债券信息-监管措施
            'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=ZQ_JGCS&TABKEY=tab1&PAGENO=1',
            # 债券信息-纪律处分-需要图像识别-先把PDF转换成图片-未处理
            # 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=ZQ_JLCF&TABKEY=tab1&PAGENO=1',
        ]
        for url in start_urls:
            if 'tab1' in url and '1800_jgxxgk' in url:
                yield scrapy.Request(url=url, callback=self.parse_jgcs, dont_filter=True, priority=5)
            elif 'tab2' in url and '1800_jgxxgk' in url:
                yield scrapy.Request(url=url, callback=self.parse_jlcf, dont_filter=True, priority=5)
            elif 'tab1' in url and 'main_wxhj' in url:
                yield scrapy.Request(url=url, callback=self.parse_wxhj_main, dont_filter=True, priority=5)
            elif 'tab2' in url and 'main_wxhj' in url:
                yield scrapy.Request(url=url, callback=self.parse_zhongxb, dont_filter=True, priority=5)
            elif 'tab3' in url and 'main_wxhj' in url:
                yield scrapy.Request(url=url, callback=self.parse_chuangyb, dont_filter=True, priority=5)
            elif '1759_cxda' in url and 'tab1' in url:
                yield scrapy.Request(url=url, callback=self.parse_cfycfjv, dont_filter=True, priority=5)
            elif '1903_detail' in url and 'first' in url:
                yield scrapy.Request(url=url, callback=self.parse_zjjgcfycf, dont_filter=True, priority=5)
            elif 'ZQ_WXHJ' in url and 'tab1' in url:
                yield scrapy.Request(url=url, callback=self.parse_zqxxwxh, dont_filter=True, priority=5)
            elif 'ZQ_JGCS' in url and 'tab1' in url:
                yield scrapy.Request(url=url, callback=self.parse_zqxxjgcs, dont_filter=True, priority=5)
            elif 'ZQ_JLCF' in url and 'tab1' in url:
                yield scrapy.Request(url=url, callback=self.parse_zqxxjlcf, dont_filter=True, priority=5)

    def parse_jgcs(self, response):
        """
        解析-监管措施
        :param response:
        :return:item
        """
        # 解析
        data_list = json.loads(response.text)
        data = data_list[0].get('data')
        if not data:
            return None
        for item in data:
            gkxx_gsdm = item.get('gkxx_gsdm')  # 公司代码
            gkxx_gsjc = item.get('gkxx_gsjc')  # 公司简称
            gkxx_gdrq = item.get('gkxx_gdrq')  # 采取监管措施日期
            gkxx_jgcs = item.get('gkxx_jgcs')  # 监管措施
            gkxx_jgsy = item.get('gkxx_jgsy')  # 函件内容
            gkxx_sjdx = item.get('gkxx_sjdx')  # 涉及对象

            jgcs_item = dict(
                regcode=gkxx_gsdm,
                bzxr=gkxx_gsjc,
                cf_type=gkxx_jgcs,
                xx_cflb=gkxx_jgcs,
                nsrlx=gkxx_sjdx,
                cf_jdrq=gkxx_gdrq,
                cf_xzjg='深圳证券交易所',
                site_id=36799,
                xxly='深圳证券交易所-数据补充',
                bz='深圳证券交易所-监管信息公开-监管措施',
            )

            if gkxx_jgsy.endswith("</a>"):
                link = re.search(r'encode-open=\'(.*?)\'', gkxx_jgsy).group(1)
                index_url = urljoin('http://reportdocs.static.szse.cn', link)
                yield scrapy.Request(url=index_url, callback=self.parse_jgcs_pdf, meta={'jgcs_item': jgcs_item}, priority=7)
            else:
                jgcs_new_item = dict(
                    oname=gkxx_gsjc,
                    cf_sy=gkxx_jgsy,
                    ws_nr_txt=gkxx_jgsy,
                    xq_url=response.url,
                )
                last_item = {**jgcs_new_item, **jgcs_item}
                yield last_item

        # 翻页请求
        pagecount = jsonpath.jsonpath(data_list[0], expr=r'$..pagecount')[0]
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, pagecount + 1):
                url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1800_jgxxgk&TABKEY=tab1&PAGENO={}&selectBkmc=0'.format(page)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_jgcs,
                    meta={'is_first': False},
                    priority=3,
                )

    def parse_jlcf(self, response):
        """
        解析-纪律处分
        :param response:
        :return:
        """
        # 解析
        results = json.loads(response.text)
        data = results[1].get('data')
        if not data:
            return None
        for item in data:
            xx_gsdm = item.get('xx_gsdm')  # 公司代码
            jc_gsjc = item.get('jc_gsjc')  # 公司简称
            xx_fwrq = item.get('xx_fwrq')  # 处分日期
            xx_cflb = item.get('xx_cflb')  # 处分类别
            xx_bt = item.get('xx_bt')  # 标题
            ck = item.get('ck')  # 查看全文
            xx_fwrq = xx_fwrq if xx_fwrq else None
            jlcf_item = dict(
                regcode=xx_gsdm,
                bzxr=jc_gsjc,
                cf_jdrq=xx_fwrq,
                cf_type=xx_cflb,
                cf_cflb=xx_cflb,
                cf_cfmc=xx_bt,

                cf_xzjg='深圳证券交易所',
                site_id=36799,
                xxly='深圳证券交易所-数据补充',
                bz='深圳证券交易所-监管信息公开-纪律处分',
            )

            if ck.endswith("</a>"):
                link = re.search(r'encode-open=\'(.*?)\'', ck).group(1)
                index_url = urljoin('http://reportdocs.static.szse.cn', link)
                if index_url.endswith('pdf'):
                    yield scrapy.Request(url=index_url, callback=self.parse_jlcf_pdf, meta={'jlcf_item': jlcf_item}, priority=7)
                elif index_url.endswith('doc'):
                    jlcf_item['xq_url'] = index_url
                    yield jlcf_item
            else:
                logger.info(f'监管信息公开-纪律处分是纯文本格式:{jlcf_item}')

        # 翻页请求
        pagecount = jsonpath.jsonpath(results[1], expr=r'$..pagecount')[0]
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, pagecount + 1):
                url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1800_jgxxgk&TABKEY=tab2&PAGENO={}&selectGsbk=0'.format(page)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_jlcf,
                    meta={'is_first': False},
                    priority=3,
                )

    def parse_jgcs_pdf(self, response):
        """
        解析-监管措施pdf
        :param response:
        :return:
        """
        jgcs_item = response.meta.get('jgcs_item')
        try:
            content_list = self.parse_pdf(response)
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []

        re_com = re.compile(r'\r|\n|\t|\s')
        content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        # print(f'pdf文本={content}')
        oname_pattern = re.compile(r'(关于对)(.*?(公司|的))')
        cf_wsh_pattern = re.compile(r'((?:监管函公司部|监管函).*?号)')
        cf_sy_pattern = re.compile(r'：(.*?(?:违反了|你的上述行为违反了|你公司的上述行为违反了))')
        cf_yj_pattern = re.compile(r'((?:违反了|你的上述行为违反了).*?规定)')
        cf_jg_pattern = re.compile(r'(现对你.*?。)')
        oname = oname_pattern.search(content)
        oname = oname.group(2) if oname else ''
        if '的' in oname:
            oname = oname.replace('的', '')
        else:
            oname = oname
        cf_wsh = cf_wsh_pattern.search(content)
        cf_wsh = cf_wsh.group(1) if cf_wsh else ''
        cf_sy = cf_sy_pattern.search(content)
        cf_sy = cf_sy.group(1) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        cf_jg = cf_jg.group(1) if cf_jg else ''
        jgcs_new_item = dict(
            oname=oname,
            cf_wsh=cf_wsh,
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=response.url,
            ws_nr_txt=content,
        )
        last_item = {**jgcs_new_item, **jgcs_item}
        # print(last_item)
        yield last_item

    def parse_jlcf_pdf(self, response):
        '''
        解析-纪律处分pdf
        :param response:
        :return:
        '''
        jlcf_item = response.meta.get('jlcf_item')
        try:
            content_list = self.parse_pdf(response)
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []

        re_com = re.compile(r'\r|\n|\t|\s')
        content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        # print(f'pdf文本={content}')
        oname_pattern = re.compile(r'(关于对)(.*?(公司|给予))')
        cf_sy_pattern = re.compile(r'(存在以下问题：|违规事实：|存在以下违规行为：)(.*?。)')
        cf_yj_pattern = re.compile(r'((违反了本所|依据本所).*?规定)')
        cf_jg_pattern = re.compile(r'((本所决定|本所作出如下处分：|本所作出如下处分决定：).*?。)')
        oname = oname_pattern.search(content)
        oname = oname.group(2) if oname else ''
        if '给予' in oname:
            oname = oname.replace('给予', '')
        else:
            oname = oname
        cf_sy = cf_sy_pattern.search(content)
        cf_sy = cf_sy.group(2) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        cf_jg = cf_jg.group(1) if cf_jg else ''
        jlcf_new_item = dict(
            oname=oname,
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=response.url,
            ws_nr_txt=content,
        )
        last_item = {**jlcf_new_item, **jlcf_item}
        yield last_item

    def parse_wxhj_main(self, response):
        """
        解析-问询函件-主板模块
        :param response:
        :return:
        """
        # 解析
        results = json.loads(response.text)
        data = results[0].get('data')
        if not data:
            return None
        for item in data:
            gsdm = item.get('gsdm')  # 公司代码
            gsjc = item.get('gsjc')  # 公司简称
            fhrq = item.get('fhrq')  # 发函日期
            hjlb = item.get('hjlb')  # 函件类别
            ck = item.get('ck')  # 函件内容
            hfck = item.get('hfck')  # 公司回复 有的是空
            if hfck:
                link = re.search(r'encode-open=\'(.*?)\'>', hfck).group(1)
                img_url = urljoin("http://reportdocs.static.szse.cn", link)
            else:
                img_url = ''
            main_item = dict(
                regcode=gsdm,
                bzxr=gsjc,
                cf_jdrq=fhrq,
                cf_type=hjlb,
                cf_cflb=hjlb,
                img_url=img_url,

                cf_xzjg='深圳证券交易所',
                site_id=36799,
                xxly='深圳证券交易所-数据补充',
                bz='深圳证券交易所-监管信息公开-问询函件-主板',
            )
            meta_data = {'main_item': main_item}

            if ck.endswith("</a>"):
                link = re.search(r'encode-open=\'(.*?)\'>', ck).group(1)
                index_url = urljoin("http://reportdocs.static.szse.cn", link)
                if index_url.endswith('pdf'):
                    yield scrapy.Request(url=index_url, callback=self.parse_main_pdf, meta=meta_data, priority=7)
                elif index_url.endswith('docx'):
                    main_item['xq_url'] = index_url
                    yield main_item
                else:
                    logger.info(f'问询函件-主板模块url非文件:{index_url}')
            else:
                logger.info('没有a标签')

        # 翻页请求
        pagecount = jsonpath.jsonpath(results[0], expr=r'$..pagecount')[0]
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, pagecount + 1):
                url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=main_wxhj&TABKEY=tab1&PAGENO={}'.format(page)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_wxhj_main,
                    meta={'is_first': False},
                    priority=3,
                )

    def parse_main_pdf(self, response):
        """
        解析-问询函件-主板pdf
        :param response:
        :return:
        """
        re_com = re.compile(r'\r|\n|\t|\s')
        main_item = response.meta.get('main_item')
        main_item['xq_url'] = response.url

        try:
            content_list = self.parse_pdf(response)
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []

        content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        main_item['ws_nr_txt'] = content
        oname_pattern = re.compile(r'(关于对)(.*?(公司|给予))')
        cf_wsh_pattern = re.compile(r'((?:问询函|关注函|补充材料有关事项的函).*?号)')
        cf_sy_pattern = re.compile(r'(存在以下问题：|违规事实：|存在以下违规行为：|董事会：)(.*?。)')
        cf_yj_pattern = re.compile(r'((违反了本所|依据本所|严格遵守).*?规定)')
        cf_jg_pattern = re.compile(r'((本所决定|本所作出如下处分：|本所作出如下处分决定：|请你公司说明).*?。)')
        oname = oname_pattern.search(content)
        oname = oname.group(2) if oname else ''
        if '给予' in oname:
            oname = oname.replace('给予', '')
        else:
            oname = oname

        main_item['oname'] = oname
        cf_wsh = cf_wsh_pattern.search(content)
        main_item['cf_wsh'] = cf_wsh.group(1) if cf_wsh else ''
        cf_sy = cf_sy_pattern.search(content)
        main_item['cf_sy'] = cf_sy.group(2) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        main_item['cf_yj'] = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        main_item['cf_jg'] = cf_jg.group(1) if cf_jg else ''

        yield main_item

    def parse_zhongxb(self, response):
        """
        解析-问询函件-中小企业版模块
        :param response:
        :return:
        """
        # 解析
        results = json.loads(response.text)
        data = results[1].get('data')
        if not data:
            return None
        for item in data:
            gsdm = item.get('gsdm')  # 公司代码
            gsjc = item.get('gsjc')  # 公司简称
            fhrq = item.get('fhrq')  # 发函日期
            hjlb = item.get('hjlb')  # 函件类别
            ck = item.get('ck')  # 函件内容
            hfck = item.get('hfck')  # 公司回复
            if hfck:
                link = re.search(r'encode-open=\'(.*?)\'>', hfck).group(1)
                img_url = urljoin("http://reportdocs.static.szse.cn", link)
            else:
                img_url = ''
            zxqyb_item = dict(
                regcode=gsdm,
                bzxr=gsjc,
                cf_jdrq=fhrq,
                cf_type=hjlb,
                cf_cflb=hjlb,
                img_url=img_url,

                cf_xzjg='深圳证券交易所',
                site_id=36799,
                xxly='深圳证券交易所-数据补充',
                bz='深圳证券交易所-监管信息公开-问询函件-中小企业板',
            )

            meta_data = {'zxqyb_item': zxqyb_item}
            if ck.endswith("</a>"):
                link = re.search(r'encode-open=\'(.*?)\'>', ck).group(1)
                index_url = urljoin("http://reportdocs.static.szse.cn", link)
                if index_url.endswith('pdf'):
                    yield scrapy.Request(url=index_url, callback=self.parse_zhongxb_pdf, meta=meta_data, priority=7)
                elif index_url.endswith('docx') or index_url.endswith('doc'):
                    zxqyb_item['xq_url'] = index_url
                    yield zxqyb_item
                else:
                    logger.info(f'问询函件-中小企业版url非文件:{index_url}')
            else:
                logger.info('没有a标签')

        # 翻页请求
        pagecount = jsonpath.jsonpath(results[1], expr=r'$..pagecount')[0]
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, pagecount + 1):
                url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=main_wxhj&TABKEY=tab2&PAGENO={}'.format(page)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_zhongxb,
                    meta={'is_first': False},
                    priority=3,
                )

    def parse_zhongxb_pdf(self, response):
        """
        解析-问询函件-中小企业板PDF
        :param response:
        :return:
        """
        re_com = re.compile(r'\r|\n|\t|\s')
        zxqyb_item = response.meta.get('zxqyb_item')

        try:
            content_list = self.parse_pdf(response)
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []

        content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        oname_pattern = re.compile(r'(关于对)(.*?(公司|给予))')
        cf_wsh_pattern = re.compile(r'((?:问询函|关注函|补充材料有关事项的函).*?号)')
        cf_sy_pattern = re.compile(r'(存在以下问题：|违规事实：|存在以下违规行为：|董事会：)(.*?。)')
        cf_yj_pattern = re.compile(r'((违反了本所|依据本所|严格遵守|按照国家法律).*?规定)')
        cf_jg_pattern = re.compile(r'((本所决定|本所作出如下处分：|本所作出如下处分决定：|请你公司说明|请你公司).*?。)')
        oname = oname_pattern.search(content)
        oname = oname.group(2) if oname else ''
        if '给予' in oname:
            oname = oname.replace('给予', '')
        else:
            oname = oname

        cf_wsh = cf_wsh_pattern.search(content)
        cf_wsh = cf_wsh.group(1) if cf_wsh else ''
        cf_sy = cf_sy_pattern.search(content)
        cf_sy = cf_sy.group(2) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        cf_jg = cf_jg.group(1) if cf_jg else ''
        zxqyb_new_item = dict(
            oname=oname,
            cf_wsh=cf_wsh,
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=response.url,
            ws_nr_txt=content,
        )

        last_item = {**zxqyb_new_item, **zxqyb_item}
        # print(last_item)
        yield last_item

    def parse_chuangyb(self, response):
        """
        解析-问询函件-创业板模块
        :param response:
        :return:
        """
        # 解析
        results = json.loads(response.text)
        data = results[2].get('data')
        if not data:
            return None
        for item in data:
            gsdm = item.get('gsdm')  # 公司代码
            gsjc = item.get('gsjc')  # 公司简称
            fhrq = item.get('fhrq')  # 发函日期
            hjlb = item.get('hjlb')  # 函件类别
            ck = item.get('ck')  # 函件内容
            hfck = item.get('hfck')  # 公司回复
            if hfck:
                link = re.search(r'encode-open=\'(.*?)\'>', hfck).group(1)
                img_url = urljoin("http://reportdocs.static.szse.cn", link)
            else:
                img_url = ''
            cyb_item = dict(
                regcode=gsdm,
                bzxr=gsjc,
                cf_jdrq=fhrq,
                cf_type=hjlb,
                cf_cflb=hjlb,
                img_url=img_url,

                cf_xzjg='深圳证券交易所',
                site_id=36799,
                xxly='深圳证券交易所-数据补充',
                bz='深圳证券交易所-监管信息公开-问询函件-创业板',
            )

            meta_data = {'cyb_item': cyb_item}
            if ck.endswith("</a>"):
                link = re.search(r'encode-open=\'(.*?)\'>', ck).group(1)
                index_url = urljoin("http://reportdocs.static.szse.cn", link)
                if index_url.endswith('pdf') or index_url.endswith('PDF'):
                    yield scrapy.Request(url=index_url, callback=self.parse_chuangyb_pdf, meta=meta_data, priority=7)
                elif index_url.endswith('docx'):
                    cyb_item['xq_url'] = index_url
                    yield cyb_item
                else:
                    logger.info(f'问询函件-创业板url非文件:{index_url}')
            else:
                logger.info('没有a标签')

        # 翻页请求
        pagecount = jsonpath.jsonpath(results[2], expr=r'$..pagecount')[0]
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, pagecount + 1):
                url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=main_wxhj&TABKEY=tab3&PAGENO={}'.format(page)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_chuangyb,
                    meta={'is_first': False},
                    priority=3,
                )

    def parse_chuangyb_pdf(self, response):
        """
        解析-问询函件-创业板模块PDF
        :param response:
        :return:
        """

        re_com = re.compile(r'\r|\n|\t|\s')
        cyb_item = response.meta.get('cyb_item')

        try:
            content_list = self.parse_pdf(response)
        except (PDFSyntaxError):
            return None
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []

        content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        oname_pattern = re.compile(r'(关于对)(.*?(公司|给予))')
        cf_wsh_pattern = re.compile(r'((?:问询函|关注函|补充材料有关事项的函).*?号)')
        cf_sy_pattern = re.compile(r'(存在以下问题：|违规事实：|存在以下违规行为：|董事会：)(.*?。)')
        cf_yj_pattern = re.compile(r'((违反了本所|依据本所|严格遵守|按照国家法律).*?规定)')
        cf_jg_pattern = re.compile(r'((本所决定|本所作出如下处分：|本所作出如下处分决定：|请你公司说明|请你公司).*?。)')
        oname = oname_pattern.search(content)
        oname = oname.group(2) if oname else ''
        if '给予' in oname:
            oname = oname.replace('给予', '')
        else:
            oname = oname

        cf_wsh = cf_wsh_pattern.search(content)
        cf_wsh = cf_wsh.group(1) if cf_wsh else ''
        cf_sy = cf_sy_pattern.search(content)
        cf_sy = cf_sy.group(2) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        cf_jg = cf_jg.group(1) if cf_jg else ''
        cyb_new_item = dict(
            oname=oname,
            cf_wsh=cf_wsh,
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=response.url,
            ws_nr_txt=content,
        )

        last_item = {**cyb_new_item, **cyb_item}
        # print(last_item)
        yield last_item

    def parse_cfycfjv(self, response):
        """
        解析-上市公司诚信档案-处罚与处分记录
        :param response:
        :return:
        """
        # 解析
        results = json.loads(response.text)
        data = results[0].get('data')
        if not data:
            return None
        for item in data:
            xx_gsdm = item.get('xx_gsdm')  # 公司代码
            gsjc = item.get('gsjc')  # 公司简称
            xx_fwrq = item.get('xx_fwrq')  # 处分日期
            cflb = item.get('cflb')  # 处分类别
            dsr = item.get('dsr')  # 当事人
            bt = item.get('bt')  # 标题
            ck = item.get('ck')  # 查看全文

            cfycfjv_item = dict(
                regcode=xx_gsdm,
                bzxr=gsjc,
                cf_jdrq=xx_fwrq,
                cf_type=cflb,
                cf_cflb=cflb,
                cf_cfmc=bt,

                cf_xzjg='深圳证券交易所',
                site_id=36799,
                xxly='深圳证券交易所-数据补充',
                bz='深圳证券交易所-上市公司信息-上市公司诚信档案-处罚与纪律处分记录',
            )
            meta_data = {'cfycfjv_item': cfycfjv_item, 'dsr': dsr}

            if ck.endswith("</a>"):
                link = re.search(r'encode-open=\'(.*?)\'>', ck).group(1)
                index_url = urljoin("http://reportdocs.static.szse.cn", link)
                if index_url.endswith('pdf'):
                    yield scrapy.Request(url=index_url, callback=self.parse_cfycfjv_pdf, meta=meta_data, priority=7)
                elif index_url.endswith('doc'):
                    cfycfjv_item['xq_url'] = index_url
                    yield cfycfjv_item
                elif index_url.endswith('docx'):
                    cfycfjv_item['xq_url'] = index_url
                    yield cfycfjv_item
                else:
                    logger.info(f'处罚与处分记录url非文件:{index_url}')

        # 翻页请求
        pagecount = jsonpath.jsonpath(results[0], expr=r'$..pagecount')[0]
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, pagecount + 1):
                url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=1759_cxda&TABKEY=tab1&PAGENO={}'.format(page)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_cfycfjv,
                    meta={'is_first': False},
                    priority=3,
                )

    def parse_zjjgcfycf(self, response):
        """
        解析-上市公司诚信档案-中介机构处罚与处分信息
        :param response:
        :return:
        """
        # 解析
        results = json.loads(response.text)
        data = results[0].get('data')
        if not data:
            return None
        for item in data:
            zj_zjrm = item.get('zj_zjrm')  # 中介机构名称
            zj_cfdx = item.get('zj_cfdx')  # 中介机构类别
            zj_fwrq = item.get('zj_fwrq')  # 处分日期
            zj_cflb = item.get('zj_cflb')  # 处分类别
            zj_gsdm = item.get('zj_gsdm')  # 涉及公司代码
            zj_zjjc = item.get('zj_zjjc')  # 涉及公司简称
            zj_dsr = item.get('zj_dsr')  # 当事人
            zj_bt = item.get('zj_bt')  # 标题
            ck = item.get('ck')  # 查看全文

            zjjgcfycf_item = dict(
                lvxingqk=zj_zjrm,
                qingxing=zj_cfdx,
                regcode=zj_gsdm,
                bzxr=zj_zjjc,
                cf_jdrq=zj_fwrq,
                cf_type=zj_cflb,
                cf_cflb=zj_cflb,
                cf_cfmc=zj_bt,

                cf_xzjg='深圳证券交易所',
                site_id=36799,
                xxly='深圳证券交易所-数据补充',
                bz='深圳证券交易所-上市公司信息-上市公司诚信档案-中介机构处罚与处分信息',
            )

            meta_data = {'zjjgcfycf_item': zjjgcfycf_item, 'zj_dsr': zj_dsr}
            if ck.endswith("</a>"):
                link = re.search(r'encode-open=\'(.*?)\'>', ck).group(1)
                index_url = urljoin("http://reportdocs.static.szse.cn", link)
                if index_url.endswith('pdf'):
                    yield scrapy.Request(url=index_url, callback=self.parse_zjjgcfycf_pdf, meta=meta_data, priority=7)
                elif index_url.endswith('doc'):
                    zjjgcfycf_item['xq_url'] = index_url
                    yield zjjgcfycf_item
                elif index_url.endswith('docx'):
                    zjjgcfycf_item['xq_url'] = index_url
                    yield zjjgcfycf_item
                else:
                    logger.info(f'中介机构处罚与处分信息url非文件:{index_url}')

    def parse_cfycfjv_pdf(self, response):
        """
        解析-上市公司诚信档案-处罚与处分记录PDF
        :param response:
        :return:
        """
        re_com = re.compile(r'\r|\n|\t|\s')
        cfycfjv_item = response.meta.get('cfycfjv_item')
        dsr = response.meta.get('dsr')
        try:
            content_list = self.parse_pdf(response)
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []
        content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        cf_wsh_pattern = re.compile(r'((?:问询函|关注函|补充材料有关事项的函).*?号)')
        cf_sy_pattern = re.compile(r'(存在以下问题：|违规事实：|存在以下违规行为：|董事会：|存在以下违规事实：)(.*?。)')
        cf_yj_pattern = re.compile(r'((违反了本所|依据本所|严格遵守|按照国家法律).*?规定)')
        cf_jg_pattern = re.compile(r'((本所决定|本所作出如下处分：|本所作出如下处分决定：).*?。)')
        cf_wsh = cf_wsh_pattern.search(content)
        cf_wsh = cf_wsh.group(1) if cf_wsh else ''
        cf_sy = cf_sy_pattern.search(content)
        cf_sy = cf_sy.group(2) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        cf_jg = cf_jg.group(1) if cf_jg else ''
        cfycfjv_new_item = dict(
            cf_wsh=cf_wsh,
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=response.url,
            ws_nr_txt=content,
        )
        onames = self.deal_dsr_oname(dsr)
        for oname in onames:
            cfycfjv_item['oname'] = oname
            last_item = {**cfycfjv_new_item, **cfycfjv_item}
            # print(last_item)
            yield last_item

    def parse_zjjgcfycf_pdf(self, response):
        """
        解析-上市公司诚信档案-中介机构处罚与处分信息PDF
        :param response:
        :return:
        """
        re_com = re.compile(r'\r|\n|\t|\s')
        zjjgcfycf_item = response.meta.get('zjjgcfycf_item')
        zj_dsr = response.meta.get('zj_dsr')
        try:
            content_list = self.parse_pdf(response)
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []
        content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        cf_wsh_pattern = re.compile(r'((?:问询函|关注函|补充材料有关事项的函).*?号)')
        cf_sy_pattern = re.compile(r'(存在以下问题：|违规事实：|存在以下违规行为：|董事会：|存在以下违规事实：)(.*?。)')
        cf_yj_pattern = re.compile(r'((违反了本所|依据本所|严格遵守|按照国家法律).*?规定)')
        cf_jg_pattern = re.compile(r'((本所决定|本所作出如下处分：|本所作出如下处分决定：).*?。)')
        cf_wsh = cf_wsh_pattern.search(content)
        cf_wsh = cf_wsh.group(1) if cf_wsh else ''
        cf_sy = cf_sy_pattern.search(content)
        cf_sy = cf_sy.group(2) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        cf_jg = cf_jg.group(1) if cf_jg else ''
        zjjgcfycf_new_item = dict(
            cf_wsh=cf_wsh,
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=response.url,
            ws_nr_txt=content,
        )
        onames = self.deal_dsr_oname(zj_dsr)
        for oname in onames:
            zjjgcfycf_item['oname'] = oname
            last_item = {**zjjgcfycf_new_item, **zjjgcfycf_item}
            # print(last_item)
            yield last_item

    def parse_zqxxwxh(self, response):
        """
        解析-债券信息-问询函
        :param response:
        :return:
        """
        # 解析
        results = json.loads(response.text)
        data = results[0].get('data')
        if not data:
            return None
        for item in data:
            dx = item.get('dx')  # 问询对象
            lx = item.get('lx')  # 类型
            hh = item.get('hh')  # 函号
            hjbt = item.get('hjbt')  # 函件标题
            fhrq = item.get('fhrq')  # 发函日期
            sjzh = item.get('sjzh')  # 涉及债券
            if hjbt:
                cf_cfmc = re.search(r'(关于对.*?)(</a>)', hjbt)
                cf_cfmc = cf_cfmc.group(1) if cf_cfmc else ''
            else:
                cf_cfmc = ''
            wxh_item = dict(
                oname=dx,
                cf_wsh=hh,
                cf_cfmc=cf_cfmc,
                regcode=sjzh,
                cf_jdrq=fhrq,
                cf_type=lx,
                cf_cflb=lx,

                cf_xzjg='深圳证券交易所',
                site_id=36799,
                xxly='深圳证券交易所-数据补充',
                bz='深圳证券交易所-债券信息-问询函',
            )
            meta_data = {'wxh_item': wxh_item}
            if hjbt.endswith("</a>"):
                link = re.search(r'encode-open=\'(.*?)\'>', hjbt).group(1)
                index_url = urljoin("http://reportdocs.static.szse.cn", link)
                if index_url.endswith('pdf'):
                    yield scrapy.Request(url=index_url, callback=self.parse_zqxxwxh_pdf, meta=meta_data, priority=7)
                elif index_url.endswith('doc'):
                    wxh_item['xq_url'] = index_url
                    yield wxh_item
                elif index_url.endswith('docx'):
                    wxh_item['xq_url'] = index_url
                    yield wxh_item
                else:
                    logger.info(f'债券信息-问询函url非文件:{index_url}')

        # 翻页请求
        pagecount = jsonpath.jsonpath(results[0], expr=r'$..pagecount')[0]
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, pagecount + 1):
                url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=ZQ_WXHJ&TABKEY=tab1&PAGENO={}'.format(page)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_zqxxwxh,
                    meta={'is_first': False},
                    priority=3,
                )

    def parse_zqxxjgcs(self, response):
        """
        解析-债券信息-监管措施
        :param response:
        :return:
        """
        # 解析
        results = json.loads(response.text)
        data = results[0].get('data')
        if not data:
            return None
        for item in data:
            dx = item.get('dx')  # 监管对象
            lx = item.get('lx')  # 类型
            hh = item.get('hh')  # 函号
            hjbt = item.get('hjbt')  # 函件标题
            fhrq = item.get('fhrq')  # 发函日期
            sjzh = item.get('sjzh')  # 涉及债券
            if hjbt:
                cf_cfmc = re.search(r'(关于对.*?)(</a>)', hjbt)
                cf_cfmc = cf_cfmc.group(1) if cf_cfmc else ''
            else:
                cf_cfmc = ''

            jgcs_item = dict(
                oname=dx,
                cf_wsh=hh,
                cf_cfmc=cf_cfmc,
                regcode=sjzh,
                cf_jdrq=fhrq,
                cf_type=lx,
                cf_cflb=lx,

                cf_xzjg='深圳证券交易所',
                site_id=36799,
                xxly='深圳证券交易所-数据补充',
                bz='深圳证券交易所-债券信息-监管措施',
            )
            meta_data = {'jgcs_item': jgcs_item}
            if hjbt.endswith("</a>"):
                link = re.search(r'encode-open=\'(.*?)\'>', hjbt).group(1)
                index_url = urljoin("http://reportdocs.static.szse.cn", link)
                if index_url.endswith('pdf'):
                    yield scrapy.Request(url=index_url, callback=self.parse_zqxxjgcs_pdf, meta=meta_data, priority=7)
                elif index_url.endswith('doc'):
                    jgcs_item['xq_url'] = index_url
                    yield jgcs_item
                elif index_url.endswith('docx'):
                    jgcs_item['xq_url'] = index_url
                    yield jgcs_item
                else:
                    logger.info(f'债券信息-监管措施url非文件:{index_url}')

        # 翻页请求
        pagecount = jsonpath.jsonpath(results[0], expr=r'$..pagecount')[0]
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, pagecount + 1):
                url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=ZQ_JGCS&TABKEY=tab1&PAGENO={}'.format(page)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_zqxxjgcs,
                    meta={'is_first': False},
                    priority=3,
                )

    def parse_zqxxjlcf(self, response):
        """
        解析-债券信息-纪律处分
        :param response:
        :return:
        """
        # 解析
        results = json.loads(response.text)
        data = results[0].get('data')
        if not data:
            return None
        for item in data:
            dx = item.get('dx')  # 处分对象
            lx = item.get('lx')  # 类型
            hh = item.get('hh')  # 函号
            hjbt = item.get('hjbt')  # 函件标题
            fhrq = item.get('fhrq')  # 发函日期
            sjzh = item.get('sjzh')  # 涉及债券
            if hjbt:
                cf_cfmc = re.search(r'(关于对.*?)(</a>)', hjbt)
                cf_cfmc = cf_cfmc.group(1) if cf_cfmc else ''
            else:
                cf_cfmc = ''

            jlcf_item = dict(
                oname=dx,
                cf_wsh=hh,
                cf_cfmc=cf_cfmc,
                regcode=sjzh,
                cf_jdrq=fhrq,
                cf_type=lx,
                cf_cflb=lx,

                cf_xzjg='深圳证券交易所',
                site_id=36799,
                xxly='深圳证券交易所-数据补充',
                bz='深圳证券交易所-债券信息-纪律处分',
            )
            meta_data = {'jlcf_item': jlcf_item}
            if hjbt.endswith("</a>"):
                link = re.search(r'encode-open=\'(.*?)\'>', hjbt).group(1)
                index_url = urljoin("http://reportdocs.static.szse.cn", link)
                if index_url.endswith('pdf'):
                    yield scrapy.Request(url=index_url, callback=self.parse_zqxxjlcf_pdf, meta=meta_data, priority=7)
                elif index_url.endswith('doc'):
                    jlcf_item['xq_url'] = index_url
                    yield jlcf_item
                elif index_url.endswith('docx'):
                    jlcf_item['xq_url'] = index_url
                    yield jlcf_item
                else:
                    logger.info(f'债券信息-纪律处分url非文件:{index_url}')

        # 翻页请求
        pagecount = jsonpath.jsonpath(results[0], expr=r'$..pagecount')[0]
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, pagecount + 1):
                url = 'http://www.szse.cn/api/report/ShowReport/data?SHOWTYPE=JSON&CATALOGID=ZQ_JLCF&TABKEY=tab1&PAGENO={}'.format(page)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_zqxxjlcf,
                    meta={'is_first': False},
                    priority=3,
                )

    def parse_zqxxwxh_pdf(self, response):
        """
        解析-债券信息-问询函PDF
        :param response:
        :return:
        """

        re_com = re.compile(r'\r|\n|\t|\s')
        wxh_item = response.meta.get('wxh_item')
        try:
            content_list = self.parse_pdf(response)
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []
        content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        cf_sy_pattern = re.compile(r'(存在以下问题：|违规事实：|存在以下违规行为：|董事会：|存在以下违规事实：|存在以下不规范事项：|存在以下信息披露不规范事项：|存在以下事项：)(.*?。)')
        cf_yj_pattern = re.compile(r'((违反了本所|依据本所|严格遵守|按照国家法律|不规范的行为违反了).*?规定)')
        cf_jg_pattern = re.compile(r'((本所决定|本所作出如下处分：|本所作出如下处分决定：|请你公司高度重视信息披露义务|提醒你公司严格遵守).*?。)')
        cf_sy = cf_sy_pattern.search(content)
        cf_sy = cf_sy.group(2) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        cf_jg = cf_jg.group(1) if cf_jg else ''
        zqxx_new_item = dict(
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=response.url,
            ws_nr_txt=content,
        )
        last_item = {**wxh_item, **zqxx_new_item}
        # print(last_item)
        yield last_item

    def parse_zqxxjgcs_pdf(self, response):
        """
        解析-债券信息-监管措施PDF
        :param response:
        :return:
        """
        re_com = re.compile(r'\r|\n|\t|\s')
        jgcs_item = response.meta.get('jgcs_item')
        try:
            content_list = self.parse_pdf(response)
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []
        content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        cf_sy_pattern = re.compile(r'(存在以下问题：|违规事实：|存在以下违规行为：|董事会：|存在以下违规事实：|存在以下不规范事项：|存在以下信息披露不规范事项：|存在以下事项：)(.*?。)')
        cf_yj_pattern = re.compile(r'((违反了本所|依据本所|严格遵守|按照国家法律|不规范的行为违反了|请说明是否按照).*?规定)')
        cf_jg_pattern = re.compile(r'((本所决定|本所作出如下处分：|本所作出如下处分决定：|请你公司高度重视信息披露义务|提醒你公司严格遵守).*?。)')
        cf_sy = cf_sy_pattern.search(content)
        cf_sy = cf_sy.group(2) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        cf_jg = cf_jg.group(1) if cf_jg else ''
        jgcs_new_item = dict(
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=response.url,
            ws_nr_txt=content,
        )
        last_item = {**jgcs_item, **jgcs_new_item}
        # print(last_item)
        yield last_item

    def parse_zqxxjlcf_pdf(self, response):
        """
        解析-债券信息-纪律处分PDF
        :param response:
        :return:
        """
        pass

    def parse_pdf(self, response):
        """
        解析PDF文件
        :param response:
        :return:
        """
        # 用文件对象来创建一个pdf文档分析器
        praser = PDFParser(BytesIO(response.body))
        # 创建一个PDF文档
        doc = PDFDocument()
        # 连接分析器 与文档对象
        praser.set_document(doc)
        doc.set_parser(praser)
        # 提供初始化密码
        # 如果没有密码 就创建一个空的字符串
        doc.initialize()
        # 检测文档是否提供txt转换，不提供就忽略
        if not doc.is_extractable:
            raise PDFTextExtractionNotAllowed
        else:
            # 创建PDf 资源管理器 来管理共享资源
            rsrcmgr = PDFResourceManager()
            # 创建一个PDF设备对象
            laparams = LAParams()
            device = PDFPageAggregator(rsrcmgr, laparams=laparams)
            # 创建一个PDF解释器对象
            interpreter = PDFPageInterpreter(rsrcmgr, device)
            contents_list = []
            # 循环遍历列表，每次处理一个page的内容
            for page in doc.get_pages():  # doc.get_pages() 获取page列表
                # 接受该页面的LTPage对象
                interpreter.process_page(page)
                # 这里layout是一个LTPage对象 里面存放着
                # 这个page解析出的各种对象 一般包括LTTextBox, LTFigure, LTImage, LTTextBoxHorizontal 等等
                # 想要获取文本就获得对象的text属性
                layout = device.get_result()
                for index, out in enumerate(layout):
                    if isinstance(out, LTTextBox):
                        contents = out.get_text().strip()
                        contents_list.append(contents)
            return contents_list

    def deal_dsr_oname(self, dsr):
        """
        把当事人拆解开
        :param dsr:
        :return:
        """
        if not dsr:
            return ""
        results = dsr.split("、")
        for result in results:
            yield result