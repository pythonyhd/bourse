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


class ShanghaiStockSpider(scrapy.Spider):
    name = 'shanghai_stock'
    allowed_domains = ['sse.com.cn']

    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'bourse.middlewares.RandomUserAgentMiddleware': 120
        },
        'ITEM_PIPELINES': {
            'bourse.pipelines.BoursePipeline': 300,
            'bourse.pipelines.DealShanghaiFilesPipeline': 330,
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
        "DOWNLOAD_FAIL_ON_DATALOSS": False,
    }

    def start_requests(self):
        """
        入口
        :return:
        """
        # 监管信息公开请求头
        headers_jgxxgk = {
            "DNT": "1",
            "Connection": "keep-alive",
            "Referer": "http://www.sse.com.cn/disclosure/credibility/supervision/measures/",
        }
        # 债券监管请求头
        headers_zqjg = {
            "X-Requested-With": "XMLHttpRequest",
            "DNT": "1",
            "Host": "www.sse.com.cn",
            "Origin": "http://www.sse.com.cn",
            "Connection": "keep-alive",
            "Referer": "http://www.sse.com.cn/disclosure/credibility/bonds/regulatory/",
        }
        # 交易监管请求头
        headers_jyjg = {
            "X-Requested-With": "XMLHttpRequest",
            "DNT": "1",
            "Host": "www.sse.com.cn",
            "Origin": "http://www.sse.com.cn",
            "Connection": "keep-alive",
            "Referer": "http://www.sse.com.cn/disclosure/credibility/regulatory/punishment/",
        }
        # 会员及其他交易参与人监管请求头
        headers_huiyuan = {
            "X-Requested-With": "XMLHttpRequest",
            "DNT": "1",
            "Host": "www.sse.com.cn",
            "Origin": "http://www.sse.com.cn",
            "Connection": "keep-alive",
            "Referer": "http://www.sse.com.cn/disclosure/credibility/members/disposition/",
        }
        start_urls = [
            # 监管信息公开-公司监管-监管措施(PDF+HTML+DOC混合)
            'http://query.sse.com.cn/commonSoaQuery.do?siteId=28&sqlId=BS_KCB_GGLL&channelId=10007%2C10008%2C10009%2C10010&order=createTime%7Cdesc%2Cstockcode%7Casc&isPagination=true&pageHelp.pageSize=15&pageHelp.pageNo=1',
            # 监管信息公开-公司监管-监管问询(只有PDF)
            'http://query.sse.com.cn/commonSoaQuery.do?siteId=28&sqlId=BS_KCB_GGLL&channelId=10743%2C10744%2C10012&order=createTime%7Cdesc%2Cstockcode%7Casc&isPagination=true&pageHelp.pageSize=15&pageHelp.pageNo=1',
            # 债券监管-债券监管措施
            'http://www.sse.com.cn/disclosure/credibility/bonds/regulatory/s_index.htm',
            # 债券监管-债券纪律处分
            'http://www.sse.com.cn/disclosure/credibility/bonds/disposition/s_index.htm',
            # 交易监管-纪律处分
            'http://www.sse.com.cn/disclosure/credibility/regulatory/punishment/s_index.htm',
            # 会员及其他交易参与人监管-纪律处分
            'http://www.sse.com.cn/disclosure/credibility/members/disposition/s_index.htm',
        ]

        for url in start_urls:
            if '10007' in url:
                yield scrapy.Request(url=url, headers=headers_jgxxgk, callback=self.parse_gsjg_jgcs, dont_filter=True, priority=5)
            elif '10743' in url:
                yield scrapy.Request(url=url, headers=headers_jgxxgk, callback=self.parse_gsjg_jgwx, dont_filter=True, priority=5)
            elif 'bonds' in url and 'regulatory' in url:
                yield scrapy.Request(url=url, headers=headers_zqjg, method='POST', callback=self.parse_zqjg_jgcs, dont_filter=True, priority=5)
            elif 'bonds' in url and 'disposition' in url:
                yield scrapy.Request(url=url, headers=headers_zqjg, method='POST', callback=self.parse_zqjg_jlcf, dont_filter=True, priority=5)
            elif 'regulatory' in url and 'punishment' in url:
                yield scrapy.Request(url=url, headers=headers_jyjg, method='POST', callback=self.parse_jyjg_jlcf, dont_filter=True, priority=5)
            elif 'members' in url and 'disposition' in url:
                yield scrapy.Request(url=url, headers=headers_huiyuan, method='POST', callback=self.parse_huiyuan, dont_filter=True, priority=5)

    def parse_gsjg_jgcs(self, response):
        """ 解析-监管信息公开-公司监管-监管措施 """
        # 解析
        results = json.loads(response.text)
        # 获取翻页页码
        data = results.get('pageHelp')
        if not data:
            return None
        # 数据获取
        result = results.get('result')
        if not result:
            return None
        for item in result:
            extSECURITY_CODE = item.get('extSECURITY_CODE')  # 证券代码
            extGSJC = item.get('extGSJC')  # 证券简称
            extTYPE = item.get('extTYPE')  # 监管类型
            docTitle = item.get('docTitle')  # 处理事由
            extTeacher = item.get('extTeacher')  # 涉及对象
            cmsOpDate = item.get('cmsOpDate')  # 处理日期
            createTime = item.get('createTime')  # 发布日期
            docURL = item.get('docURL')  # 文件下载链接
            docURL = "http://" + str(docURL)

            base_item = dict(
                regcode=extSECURITY_CODE,
                bzxr=extGSJC,
                cf_type=extTYPE,
                cf_cflb=extTYPE,
                nsrlx=extTeacher,
                fb_rq=cmsOpDate[:10],
                cf_jdrq=createTime[:10],
                cf_xzjg='上海证券交易所',
                site_id=36921,
                xxly='上海证券交易所-数据补充',
                bz='上海证券交易所-监管信息公开-公司监管-监管措施',
            )

            if docURL.endswith('pdf'):
                base_item['cf_cfmc'] = docTitle
                yield scrapy.Request(url=docURL, callback=self.parse_gsjg_jgcs_pdf, meta={'base_item': base_item}, priority=7)
            elif docURL.endswith('doc') or docURL.endswith('docx'):
                base_item['xq_url'] = docURL
                yield base_item
            elif docURL.endswith('shtml'):
                base_item['cf_cfmc'] = docTitle
                yield scrapy.Request(url=docURL, callback=self.parse_detail, meta={'base_item': base_item}, priority=7)
            else:
                # 网站公示数据没有详情，数据基本上没有用
                logger.debug(f'监管信息公开-公司监管-监管措施-没有源文件:{docURL}--{item}')
                base_item['cf_cfmc'] = docTitle
                base_item['xq_url'] = response.url
                base_item['oname'] = extGSJC
                yield base_item

        # 翻页请求
        pageCount = jsonpath.jsonpath(data, expr=r'$..pageCount')[0]
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, pageCount + 1):
                url = 'http://query.sse.com.cn/commonSoaQuery.do?siteId=28&sqlId=BS_KCB_GGLL&channelId=10007%2C10008%2C10009%2C10010&order=createTime%7Cdesc%2Cstockcode%7Casc&isPagination=true&pageHelp.pageSize=15&pageHelp.pageNo={}'.format(page)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_gsjg_jgcs,
                    meta={'is_first': False},
                    priority=3,
                )

    def parse_gsjg_jgwx(self, response):
        """ 解析-监管信息公开-公司监管-监管问询 """
        # 解析
        results = json.loads(response.text)
        # 获取翻页页码
        data = results.get('pageHelp')
        if not data:
            return None
        # 数据获取
        result = results.get('result')
        if not result:
            return None
        for item in result:
            extSECURITY_CODE = item.get('extSECURITY_CODE')  # 公司代码
            extGSJC = item.get('extGSJC')  # 公司简称
            cmsOpDate = item.get('cmsOpDate')  # 发函日期
            extWTFL = item.get('extWTFL')  # 监管问询类型
            docTitle = item.get('docTitle')  # 标题
            createTime = item.get('createTime')  # 发布日期
            docURL = item.get('docURL')  # 文件下载链接
            docURL = "http://" + str(docURL)

            base_item = dict(
                regcode=extSECURITY_CODE,
                bzxr=extGSJC,
                cf_type=extWTFL,
                cf_cflb=extWTFL,
                fb_rq=cmsOpDate[:10],
                cf_jdrq=createTime[:10],
                cf_xzjg='上海证券交易所',
                site_id=36921,
                xxly='上海证券交易所-数据补充',
                bz='上海证券交易所-监管信息公开-公司监管-监管问询',
            )

            if docURL.endswith('pdf'):
                base_item['cf_cfmc'] = docTitle
                yield scrapy.Request(url=docURL, callback=self.parse_gsjg_jgwx_pdf, meta={'base_item': base_item}, priority=7)
            else:
                # 网站公示数据没有详情，数据基本上没有用
                logger.info(f'监管信息公开-公司监管-监管措施-没有源文件:{docURL}--{item}')
                base_item['cf_cfmc'] = docTitle
                base_item['xq_url'] = response.url
                base_item['oname'] = extGSJC
                yield base_item

        # 翻页请求
        pageCount = jsonpath.jsonpath(data, expr=r'$..pageCount')[0]
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, pageCount + 1):
                url = 'http://query.sse.com.cn/commonSoaQuery.do?siteId=28&sqlId=BS_KCB_GGLL&channelId=10743%2C10744%2C10012&order=createTime%7Cdesc%2Cstockcode%7Casc&isPagination=true&pageHelp.pageSize=15&pageHelp.pageNo={}'.format(page)
                yield scrapy.Request(
                    url=url,
                    callback=self.parse_gsjg_jgwx,
                    meta={'is_first': False},
                    priority=3,
                )

    def parse_detail(self, response):
        """ 解析纯HTML """
        base_item = response.meta.get('base_item')
        cf_cfmc = base_item.get('cf_cfmc')
        re_com = re.compile(r'\r|\n|\t|\s')
        selector = scrapy.Selector(text=response.text)
        # cf_cfmc = selector.xpath('//div[@class="article-infor"]/h2/text()').get('')
        cf_wsh = selector.css('div[class=allZoom] p:nth-child(1)::text').get('')
        ws_nr_list = selector.xpath('//div[@class="article-infor"]//text()').getall()
        ws_nr_txt = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in ws_nr_list])
        oname = re.search(r'(关于要求|关于对|关于)(.*?公司)', cf_cfmc)
        oname = oname.group(2) if oname else ''
        cf_sy_pattern = re.compile(r'(经查明.*?。)')
        cf_yj_pattern = re.compile(r'((上述行为违反了|违反了|根据).*?(有关|的)规定)')
        cf_jg_pattern = re.compile(r'((公司应当|公司应|希望公司).*?。)')
        cf_sy = cf_sy_pattern.search(ws_nr_txt)
        cf_sy = cf_sy.group(1) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(ws_nr_txt)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(ws_nr_txt)
        cf_jg = cf_jg.group(1) if cf_jg else ''
        item_detail = dict(
            oname=oname,
            cf_wsh=cf_wsh,
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=response.url,
            ws_nr_txt=ws_nr_txt,
        )
        item = {**item_detail, **base_item}
        # print(item)
        yield item

    def parse_zqjg_jgcs(self, response):
        """ 债券监管-债券监管措施-解析翻页HTML """
        # 列表解析
        selector = scrapy.Selector(text=response.text)
        base_table = selector.xpath('//table[@class="table "]/tbody/tr')
        for td in base_table:
            regcode = td.css('td:first-child::text').get('')  # 证券代码
            bzxr = td.css('td:nth-child(2)::text').get('')  # 证券简称
            cf_type = td.css('td:nth-child(3)::text').get('')  # 监管类型
            cf_cflb = td.css('td:nth-child(3)::text').get('')  # 监管类型
            cf_cfmc = td.css('td:nth-child(4) a::attr(title)').get('')  # 标题
            oname = td.css('td:nth-child(5)::text').get('')  # 涉及对象
            cf_jdrq = td.css('td:last-child::text').get('')  # 处分日期
            xq_url = td.css('td:nth-child(4) a::attr(href)').get('')  # 详情url
            xq_url = urljoin(response.url, xq_url)

            base_item = dict(
                oname=oname,
                cf_cfmc=cf_cfmc,
                cf_jdrq=cf_jdrq,
                cf_type=cf_type,
                cf_cflb=cf_cflb,
                regcode=regcode,
                bzxr=bzxr,
            )
            if not xq_url:
                return None
            yield scrapy.Request(url=xq_url, callback=self.parse_zqjg_jgcs_detail, meta={'base_item': base_item}, priority=7)

        # 列表翻页
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, 11):
                url = 'http://www.sse.com.cn/disclosure/credibility/bonds/regulatory/s_index_{}.htm'.format(page)
                yield scrapy.Request(url=url, callback=self.parse_zqjg_jgcs, meta={'is_first': False}, priority=3)

    def parse_zqjg_jlcf(self, response):
        """ 债券监管-债券纪律处分-解析翻页HTML """
        # 列表解析
        selector = scrapy.Selector(text=response.text)
        base_table = selector.xpath('//table[@class="table"]/tbody/tr')
        for td in base_table:
            regcode = td.css('td:first-child::text').get('')  # 证券代码
            bzxr = td.css('td:nth-child(2)::text').get('')  # 证券简称
            cf_cfmc = td.css('td:nth-child(3) div a::attr(title)').get('')  # 标题
            cf_jdrq = td.css('td:last-child::text').get('')  # 处分日期
            xq_url = td.css('td:nth-child(3) div a::attr(href)').get('')  # 详情url
            xq_url = urljoin(response.url, xq_url)
            oname = re.search(r'(?:关于对|关于)对?(.*?公司)', cf_cfmc)
            oname = oname.group(1) if oname else ''

            base_item = dict(
                oname=oname,
                cf_cfmc=cf_cfmc,
                cf_jdrq=cf_jdrq,
                cf_type='债券监管-债券纪律处分',
                cf_cflb='债券监管-债券纪律处分',
                regcode=regcode,
                bzxr=bzxr,
            )
            if not xq_url:
                return None
            if xq_url.endswith('doc') or xq_url.endswith('docx'):
                base_item['cf_xzjg'] = '上海证券交易所',
                base_item['site_id'] = 36921,
                base_item['xxly'] = '上海证券交易所-数据补充',
                base_item['bz'] = '上海证券交易所-债券监管-债券监管措施',
                base_item['xq_url'] = xq_url
                yield base_item
            else:
                yield scrapy.Request(url=xq_url, callback=self.parse_zqjg_jlcf_detail, meta={'base_item': base_item}, priority=7)

        # 列表翻页
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, 3):
                url = 'http://www.sse.com.cn/disclosure/credibility/bonds/disposition/s_index_{}.htm'.format(page)
                yield scrapy.Request(url=url, callback=self.parse_zqjg_jlcf, meta={'is_first': False}, priority=3)

    def parse_jyjg_jlcf(self, response):
        """ 交易监管-纪律处分-解析列表HTML与翻页 """
        # 列表解析
        base_item = dict(
            cf_xzjg='上海证券交易所',
            site_id=36921,
            xxly='上海证券交易所-数据补充',
            bz='上海证券交易所-交易监管-纪律处分',
        )
        selector = scrapy.Selector(text=response.text)
        base_div = selector.xpath('//dl/dd')
        for data in base_div:
            cf_jdrq = data.css('span::text').get('')  # 日期
            cf_cfmc = data.css('a::attr(title)').get('')  # 名称
            xq_url = data.css('a::attr(href)').get('')  # 详情链接
            xq_url = urljoin(response.url, xq_url)
            oname_first = re.search(r'(关于对|关于给与)(.*?公司)', cf_cfmc)
            oname_second = re.search(r'(关于对|关于给与)(.*?)(名下)', cf_cfmc)
            oname_one = oname_first.group(2) if oname_first else ''
            oname_two = oname_second.group(2) if oname_second else ''
            oname = oname_one if oname_one else oname_two
            jlcf_item = dict(
                oname=oname,
                cf_cfmc=cf_cfmc,
                cf_jdrq=cf_jdrq,
                fb_rq=cf_jdrq,
                cf_type='交易监管-纪律处分',
                cf_cflb='交易监管-纪律处分',
            )
            meta_data = {**base_item, **jlcf_item}
            if not xq_url:
                return None
            if xq_url.endswith('doc') or xq_url.endswith('docx'):
                meta_data['xq_url'] = xq_url
                yield meta_data
            else:
                yield scrapy.Request(url=xq_url, callback=self.parse_jyjg_jlcf_detail, meta={'base_item': meta_data}, priority=7)

        # 列表翻页
        is_first = response.meta.get('is_first', True)
        if is_first:
            for page in range(2, 5):
                url = 'http://www.sse.com.cn/disclosure/credibility/regulatory/punishment/s_index_{}.htm'.format(page)
                yield scrapy.Request(url=url, callback=self.parse_jyjg_jlcf, meta={'is_first': False}, priority=3)

    def parse_huiyuan(self, response):
        """ 会员及其他交易参与人监管-纪律处分-解析 """
        # 解析列表
        base_item = dict(
            cf_xzjg='上海证券交易所',
            site_id=36921,
            xxly='上海证券交易所-数据补充',
            bz='上海证券交易所-会员及其他交易参与人监管-纪律处分',
        )
        selector = scrapy.Selector(text=response.text)
        base_table = selector.xpath('//table[@class="table"]/tbody/tr')
        for td in base_table:
            cf_type = td.css('td:first-child::text').get('')  # 监管类型
            cf_cfmc = td.css('td:nth-child(2) div a::attr(title)').get('')  # 标题
            oname = td.css('td:nth-child(3)::text').get('')  # 涉及对象
            cf_jdrq = td.css('td:last-child::text').get('')  # 处分日期
            xq_url = td.css('td:nth-child(2) div a::attr(href)').get('')  # 详情url
            xq_url = urljoin(response.url, xq_url)

            jlcf_item = dict(
                oname=oname,
                cf_cfmc=cf_cfmc,
                cf_jdrq=cf_jdrq,
                fb_rq=cf_jdrq,
                cf_type=cf_type,
                cf_cflb=cf_type,
            )
            meta_data = {**base_item, **jlcf_item}
            if not xq_url:
                return None
            if xq_url.endswith('pdf') or xq_url.endswith('PDF'):
                yield scrapy.Request(url=xq_url, callback=self.parse_huiyuan_pdf, meta={'base_item': meta_data}, priority=7)
            else:
                logger.info("会员及其他交易参与人监管-不是PDF文件")

    def parse_gsjg_jgcs_pdf(self, response):
        """ 解析PDF，提取数据 """
        re_com = re.compile(r'\r|\n|\t|\s')
        base_item = response.meta.get('base_item')
        cf_cfmc = base_item.get('cf_cfmc')
        try:
            content_list = self.parse_pdf(response)
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []
        content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        oname_pattern = re.compile(r'(当事人：)(.*?)(，)')
        cf_wsh_pattern = re.compile(r'(.*?号)(关于|关于对)')
        cf_sy_pattern = re.compile(r'((?:存在以下违规事项：|你的上述行为违反了|经查明).*?。)')
        cf_yj_pattern = re.compile(r'((公司上述行为违反了|你的上述行为违反了|根据).*?规定)')
        cf_jg_pattern = re.compile(r'((做出如下纪律处分决定：|公司应当|做出如下监管措施决定：).*?。)')
        oname = oname_pattern.search(content)
        oname = oname.group(2) if oname else ''

        if oname:
            oname = oname
        else:
            oname = re.search(r'(关于对|关于)(.*?公司)', cf_cfmc)
            oname = oname.group(2) if oname else ''
        cf_wsh = cf_wsh_pattern.search(content)
        cf_wsh = cf_wsh.group(1) if cf_wsh else ''
        cf_wsh = cf_wsh.replace('上海证券交易所', '') if '上海证券交易所' in cf_wsh else cf_wsh
        cf_sy = cf_sy_pattern.search(content)
        cf_sy = cf_sy.group(1) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        cf_jg = cf_jg.group(1) if cf_jg else ''

        item_detail = dict(
            oname=oname,
            cf_wsh=cf_wsh,
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=response.url,
            ws_nr_txt=content,
        )
        item = {**item_detail, **base_item}
        # print(item)
        yield item

    def parse_gsjg_jgwx_pdf(self, response):
        """ 解析-监管信息公开-公司监管-监管问询PDF """
        re_com = re.compile(r'\r|\n|\t|\s')
        base_item = response.meta.get('base_item')
        cf_cfmc = base_item.get('cf_cfmc')
        try:
            content_list = self.parse_pdf(response)
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []
        content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        oname_pattern = re.compile(r'(关于对|关于)(.*?公司)')
        cf_wsh_pattern = re.compile(r'(上海证券交易所)(.*?号)(关于)')
        cf_sy_pattern = re.compile(r'((?:存在以下违规事项：|你的上述行为违反了|经查明|经审阅).*?。)')
        cf_yj_pattern = re.compile(r'((公司上述行为违反了|你的上述行为违反了|根据|依据).*?规定)')
        cf_jg_pattern = re.compile(r'((做出如下纪律处分决定：|公司应当|做出如下监管措施决定：|请你公司).*?。)')
        oname = oname_pattern.search(cf_cfmc)
        oname = oname.group(2) if oname else ''
        # 文书号读取有问题
        cf_wsh = cf_wsh_pattern.search(content)
        cf_wsh = cf_wsh.group(2) if cf_wsh else ''
        cf_wsh = cf_wsh.replace('上海证券交易所', '') if '上海证券交易所' in cf_wsh else cf_wsh
        cf_sy = cf_sy_pattern.search(content)
        cf_sy = cf_sy.group(1) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        cf_jg = cf_jg.group(1) if cf_jg else ''

        item_detail = dict(
            oname=oname,
            cf_wsh=cf_wsh,
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=response.url,
            ws_nr_txt=content,
        )
        item = {**item_detail, **base_item}
        # print(item)
        yield item

    def parse_huiyuan_pdf(self, response):
        """ 会员及其他交易参与人监管-纪律处分-解析PDF文件 """
        re_com = re.compile(r'\r|\n|\t|\s')
        base_item = response.meta.get('base_item')
        try:
            content_list = self.parse_pdf(response)
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []
        content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        cf_wsh_pattern = re.compile(r'纪律处分决定书(.*?号)')
        cf_sy_pattern = re.compile(r'(根据中国证监会.*?。)')
        cf_yj_pattern = re.compile(r'((违反了|依据).*?规定)')
        cf_jg_pattern = re.compile(r'((做出如下纪律处分决定：|公司应当|做出如下监管措施决定：|请你公司).*?。)')
        cf_sy = cf_sy_pattern.search(content)
        cf_sy = cf_sy.group(1) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        cf_jg = cf_jg.group(1) if cf_jg else ''

        cf_wsh = cf_wsh_pattern.search(content)
        cf_wsh = cf_wsh.group(1) if cf_wsh else ''

        item_detail = dict(
            cf_wsh=cf_wsh,
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=response.url,
            ws_nr_txt=content,
        )
        item = {**item_detail, **base_item}
        # print(item)
        yield item

    def parse_zqjg_jgcs_detail(self, response):
        """ 债券监管-债券监管措施-解析详情页 """
        # 详情解析
        base_item = response.meta.get('base_item')
        selector = scrapy.Selector(text=response.text)
        re_com = re.compile(r'\r|\n|\t|\s')
        xq_url = response.url
        ws_nr_list = selector.xpath('//div[@class="article-infor"]//text()').getall()
        ws_nr_text = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in ws_nr_list])
        cf_wsh = selector.xpath('//div[@class="allZoom"]/p[2]/text()').get('')
        cf_wsh_second = selector.xpath('//div[@class="allZoom"]/p[1]/text()').get('')
        cf_wsh_third = selector.xpath('//div[@class="allZoom"]/div[1]/text()').get('')
        cf_wsh = cf_wsh_second if '当事人' in cf_wsh else cf_wsh
        cf_wsh = cf_wsh_third if not cf_wsh else cf_wsh
        cf_sy_pattern = re.compile(r'((经查明|存在以下违规事项：).*?。)')
        cf_yj_pattern = re.compile(r'((违反了|根据).*?规定)')
        cf_jg_pattern = re.compile(r'((鉴于上述行为|鉴于你公司上述行为|本所将根据行为性质及情节).*?。)')
        cf_sy = cf_sy_pattern.search(ws_nr_text)
        cf_sy = cf_sy.group(1) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(ws_nr_text)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(ws_nr_text)
        cf_jg = cf_jg.group(1) if cf_jg else ''
        detail_item = dict(
            cf_wsh=cf_wsh,
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=xq_url,
            ws_nr_txt=ws_nr_text,
            cf_xzjg='上海证券交易所',
            site_id=36921,
            xxly='上海证券交易所-数据补充',
            bz='上海证券交易所-债券监管-债券监管措施',
        )
        item = {**base_item, **detail_item}
        # print(item)
        yield item

    def parse_zqjg_jlcf_detail(self, response):
        """ 债券监管-债券纪律处分-解析详情页 """
        # 详情解析
        base_item = response.meta.get('base_item')
        selector = scrapy.Selector(text=response.text)
        re_com = re.compile(r'\r|\n|\t|\s')
        xq_url = response.url
        ws_nr_list = selector.xpath('//div[@class="article-infor"]//text()').getall()
        ws_nr_txt = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in ws_nr_list])
        cf_wsh_pattern = re.compile(r'(?:纪律处分决定书)?(纪律处分决定书.*?号)')
        cf_sy_pattern = re.compile(r'(存在以下违规事实：|经查明.*?。)')
        cf_yj_pattern = re.compile(r'((根据|依据).*?规定)')
        cf_jg_pattern = re.compile(r'(做出如下纪律处分决定：.*?。)')

        cf_wsh = cf_wsh_pattern.search(ws_nr_txt)
        cf_wsh = cf_wsh.group(1) if cf_wsh else ''
        cf_sy = cf_sy_pattern.search(ws_nr_txt)
        cf_sy = cf_sy.group(1) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(ws_nr_txt)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(ws_nr_txt)
        cf_jg = cf_jg.group(1) if cf_jg else ''

        detail_item = dict(
            cf_wsh=cf_wsh,
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=xq_url,
            ws_nr_txt=ws_nr_txt,
            cf_xzjg='上海证券交易所',
            site_id=36921,
            xxly='上海证券交易所-数据补充',
            bz='上海证券交易所-债券监管-债券纪律处分',
        )
        item = {**base_item, **detail_item}
        # print(item)
        yield item

    def parse_jyjg_jlcf_detail(self, response):
        """ 交易监管-纪律处分-解析详情HTML """
        # 详情解析
        base_item = response.meta.get('base_item')
        selector = scrapy.Selector(text=response.text)
        re_com = re.compile(r'\r|\n|\t|\s')
        xq_url = response.url
        ws_nr_list = selector.xpath('//div[@class="article-infor"]//text()').getall()
        ws_nr_txt = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in ws_nr_list])

        cf_sy_pattern = re.compile(r'(经查明|经审核|经查.*?。)')
        cf_yj_pattern = re.compile(r'((根据|依据).*?规定)')
        cf_jg_pattern = re.compile(r'((做出如下纪律处分决定：|本所决定).*?。)')
        cf_sy = cf_sy_pattern.search(ws_nr_txt)
        cf_sy = cf_sy.group(1) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(ws_nr_txt)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(ws_nr_txt)
        cf_jg = cf_jg.group(1) if cf_jg else ''

        detail_item = dict(
            cf_sy=cf_sy,
            cf_yj=cf_yj,
            cf_jg=cf_jg,
            xq_url=xq_url,
            ws_nr_txt=ws_nr_txt,
            cf_xzjg='上海证券交易所',
            site_id=36921,
            xxly='上海证券交易所-数据补充',
            bz='上海证券交易所-债券监管-债券纪律处分',
        )
        item = {**base_item, **detail_item}
        # print(item)
        yield item

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