# -*- coding: utf-8 -*-
import json
import re
import time
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


class NationalStockSpider(scrapy.Spider):
    name = 'national_stock'
    allowed_domains = ['neeq.com.cn']

    custom_settings = {
        'DOWNLOADER_MIDDLEWARES': {
            'bourse.middlewares.RandomUserAgentMiddleware': 120
        },
        'ITEM_PIPELINES': {
            'bourse.pipelines.BoursePipeline': 300,
            'bourse.pipelines.DealNationalFilesPipeline': 330,
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
    base_item = dict(
        cf_xzjg='全国中小企业股份转让系统',
        site_id=37097,
        xxly='全国中小企业股份转让系统-数据补充',
    )

    def start_requests(self):
        """入口 """
        form_data_wxh = {
            "page": "1",
            "sortfield": "bean.create_time",
            "sorttype": "desc",
            "letterType": "",
            "companyCode": "",
            # "startTime": "2019-01-09",
            # "endTime": "2020-01-09",
            "keyword": "",
            "xxfcbj": "",
            "status": "0",
        }
        form_data_zljgcs = {
            "page": "1",
            "companyCd": "",
            "disclosureType": "8",
            # "startTime": "2019-01-10",
            # "endTime": "2020-01-10",
            "keyword": "",
            "sortfield": "xxssdq",
            "sorttype": "asc",
        }
        form_data_jlcf = {
            "keyword": "",
            # "startTime": "2019-01-10",
            # "endTime": "2020-01-10",
            "page": "1",
            "companyCode": "",
            "sortfield": "xxssdq",
            "sorttype": "asc",
        }
        start_urls = [
            # 监管公开信息-问询函
            'http://www.neeq.com.cn/inquiryLetterController/infoResultse.do',
            # 监管公开信息-自律监管措施
            'http://www.neeq.com.cn/disclosureInfoController/infoResult.do',
            # 监管公开信息-纪律处分
            'http://www.neeq.com.cn/PunishmentController/infoResultse.do',
        ]
        for url in start_urls:
            if 'inquiryLetterController' in url and 'infoResultse' in url:
                yield scrapy.FormRequest(
                    url=url,
                    formdata=form_data_wxh,
                    callback=self.parse_wxh,
                    meta={'form_data': form_data_wxh},
                )
            elif 'disclosureInfoController' in url and 'infoResult' in url:
                yield scrapy.FormRequest(
                    url=url,
                    formdata=form_data_zljgcs,
                    callback=self.parse_zljgcs,
                    meta={'form_data': form_data_zljgcs},
                )
            elif 'PunishmentController' in url and 'infoResultse' in url:
                yield scrapy.FormRequest(
                    url=url,
                    formdata=form_data_jlcf,
                    callback=self.parse_jlvf,
                    meta={'form_data': form_data_jlcf},
                )

    def parse_wxh(self, response):
        """ 解析-全国中小企业股份转让系统-问询函 """
        # 列表解析
        results = response.text.replace('null(', '').replace(')', '')
        results = json.loads(results)
        for result in results:
            content_list = result.get('page').get('content')
            for content in content_list:
                companyCode = content.get('companyCode')  # 代码
                companyName = content.get('companyName')  # 简称
                letterContentName = content.get('letterContentName')  # 标题
                letterTypeValue = content.get('letterTypeValue')  # 发函类别
                createTime = jsonpath.jsonpath(content, expr=r'$..createTime.time')
                lastUpdateTime = jsonpath.jsonpath(content, expr=r'$..lastUpdateTime.time')
                createTime = self.handle_timestmp(createTime[0]) if createTime else None
                lastUpdateTime = self.handle_timestmp(lastUpdateTime[0]) if lastUpdateTime else None
                companyReply = content.get('companyReply')  # 公司回复PDF链接
                letterContent = content.get('letterContent')  # 内容PDF链接
                img_url = urljoin(response.url, companyReply)
                xq_url = urljoin(response.url, letterContent)
                oname = re.search(r'(?:关于对|关于)对?(.*?公司)', letterContentName)
                oname = oname.group(1) if oname else letterContentName

                first_item = dict(
                    oname=oname,
                    cf_cfmc=letterContentName,
                    regcode=companyCode,
                    bzxr=companyName,
                    cf_type=letterTypeValue,
                    cf_cflb=letterTypeValue,
                    fb_rq=lastUpdateTime,
                    cf_jdrq=createTime,
                    bz='全国中小企业股份转让系统-监管公开信息-问询函',
                )
                base_item = {**first_item, **self.base_item}

                if xq_url.endswith('pdf') or xq_url.endswith('PDF'):
                    base_item['img_url'] = img_url
                    yield scrapy.Request(
                        url=xq_url,
                        callback=self.parse_all_pdf,
                        meta={'base_item': base_item},
                        priority=7
                    )
                elif xq_url.endswith('doc') or xq_url.endswith('docx'):
                    base_item['xq_url'] = xq_url
                    yield base_item
                else:
                    logger.info(f'不是文件格式:{xq_url}')

        # 翻页页码
        form_data = response.meta.get('form_data')
        is_first = response.meta.get('is_first', True)
        totalPages = jsonpath.jsonpath(results, expr=r'$..page.totalPages')[0]
        if is_first:
            for page in range(2, totalPages + 1):
                form_data['page'] = str(page)
                yield scrapy.FormRequest(
                    url='http://www.neeq.com.cn/inquiryLetterController/infoResultse.do',
                    formdata=form_data,
                    callback=self.parse_wxh,
                    meta={'is_first': False, 'form_data': form_data},
                    priority=3,
                )

    def parse_zljgcs(self, response):
        """ 解析-全国中小企业股份转让系统-自律监管措施 """
        # 列表解析
        results = response.text.replace('null(', '').replace(')', '')
        results = json.loads(results)
        for result in results:
            content_list = result.get('listInfo').get('content')
            for content in content_list:
                companyCd = content.get('companyCd')  # 代码
                companyName = content.get('companyName')  # 简称
                disclosureTitle = content.get('disclosureTitle')  # 标题
                publishDate = content.get('publishDate')  # 日期
                destFilePath = content.get('destFilePath')  # 内容PDF链接
                xq_url = urljoin(response.url, destFilePath)

                oname = re.search(r'(?:关于对|关于)对?(.*?公司)', disclosureTitle)
                oname = oname.group(1) if oname else disclosureTitle

                first_item = dict(
                    oname=oname,
                    cf_cfmc=disclosureTitle,
                    regcode=companyCd,
                    bzxr=companyName,
                    cf_type='监管公开信息-自律监管措施',
                    cf_cflb='监管公开信息-自律监管措施',
                    cf_jdrq=publishDate,
                    fb_rq=publishDate,
                    bz='全国中小企业股份转让系统-监管公开信息-自律监管措施',
                )
                base_item = {**first_item, **self.base_item}
                if xq_url.endswith('pdf') or xq_url.endswith('PDF'):
                    yield scrapy.Request(
                        url=xq_url,
                        callback=self.parse_all_pdf,
                        meta={'base_item': base_item},
                        priority=7,
                    )
                else:
                    logger.info(f'不是文件格式:{xq_url}')

        # 翻页页码
        form_data = response.meta.get('form_data')
        is_first = response.meta.get('is_first', True)
        totalPages = jsonpath.jsonpath(results, expr=r'$..listInfo.totalPages')[0]
        if is_first:
            for page in range(2, totalPages + 1):
                form_data['page'] = str(page)
                yield scrapy.FormRequest(
                    url='http://www.neeq.com.cn/disclosureInfoController/infoResult.do',
                    formdata=form_data,
                    callback=self.parse_zljgcs,
                    meta={'is_first': False, 'form_data': form_data},
                    priority=3,
                )

    def parse_jlvf(self, response):
        """ 解析-全国中小企业股份转让系统-纪律处分 """
        # 列表解析
        results = response.text.replace('null(', '').replace(')', '')
        results = json.loads(results)
        for result in results:
            content_list = result.get('pageList').get('content')
            for content in content_list:
                accountabilityType = content.get('accountabilityType')  # 法律身份
                accountabilityMeasures = content.get('accountabilityMeasures')  # 处分类型
                accountabilityName = content.get('accountabilityName')  # 责任主体
                announcementTitle = content.get('announcementTitle')  # 标题
                publishDate = jsonpath.jsonpath(content, expr=r'$..announcementDate.time')  # 日期
                publishDate = self.handle_timestmp(publishDate[0]) if publishDate else None
                destFilePath = content.get('destFilePath')  # 内容PDF链接
                xq_url = urljoin(response.url, destFilePath)

                first_item = dict(
                    oname=accountabilityName,
                    cf_cfmc=announcementTitle,
                    cf_type=accountabilityType,
                    cf_cflb=accountabilityMeasures,
                    fb_rq=publishDate,
                    cf_jdrq=publishDate,
                    bz='全国中小企业股份转让系统-监管公开信息-纪律处分',
                )
                base_item = {**first_item, **self.base_item}

                if xq_url.endswith('pdf') or xq_url.endswith('PDF'):
                    yield scrapy.Request(
                        url=xq_url,
                        callback=self.parse_all_pdf,
                        meta={'base_item': base_item},
                        priority=7
                    )
                else:
                    logger.info(f'不是文件格式:{xq_url}')

            # 翻页页码
            form_data = response.meta.get('form_data')
            is_first = response.meta.get('is_first', True)
            totalPages = jsonpath.jsonpath(results, expr=r'$..pageList.totalPages')[0]
            if is_first:
                for page in range(2, totalPages + 1):
                    form_data['page'] = str(page)
                    yield scrapy.FormRequest(
                        url='http://www.neeq.com.cn/PunishmentController/infoResultse.do',
                        formdata=form_data,
                        callback=self.parse_jlvf,
                        meta={'is_first': False, 'form_data': form_data},
                        priority=3,
                    )

    def parse_all_pdf(self, response):
        """ 解析监管公开信息-PDF """
        re_com = re.compile(r'\r|\n|\t|\s')
        base_item = response.meta.get('base_item')
        try:
            content_list = self.parse_pdf(response)
        except Exception as e:
            logger.error(f'解析出错{repr(e)}')
            content_list = []
        try:
            content = reduce(lambda x, y: x + y, [re_com.sub('', i) for i in content_list])
        except (TypeError):
            logger.info('聚合PDF内容出错--返回空列表')
            content = ''
        except Exception as e:
            logger.error(f'聚合出错:{repr(e)}')
            content = ''
        cf_wsh_pattern = re.compile(r'(问询函)(半年报问询函.*?号)')
        cf_wsh_pattern_second = re.compile(r'(文件)(股转系统发.*?号)')
        cf_sy_pattern = re.compile(r'((有以下违规事实：|关注到以下情况：|经查明|经审阅|请你公司补充披露以下事项：|以下违规事实：).*?。)')
        cf_yj_pattern = re.compile(r'((公司上述行为违反了|你的上述行为违反了|根据|依据).*?规定)')
        cf_jg_pattern = re.compile(r'((我司作出如下纪律处分决定：|做出如下纪律处分决定：|请就上述问题做出书面说明|收到本问询函后|做出如下决定：|特此提出警示如下：|我司作出如下决定：).*?。)')
        cf_wsh_second = cf_wsh_pattern_second.search(content)
        cf_wsh_second = cf_wsh_second.group(2) if cf_wsh_second else ''
        cf_wsh_first = cf_wsh_pattern.search(content)
        cf_wsh = cf_wsh_first.group(2) if cf_wsh_first else cf_wsh_second
        cf_sy = cf_sy_pattern.search(content)
        cf_sy = cf_sy.group(1) if cf_sy else ''
        cf_yj = cf_yj_pattern.search(content)
        cf_yj = cf_yj.group(1) if cf_yj else ''
        cf_jg = cf_jg_pattern.search(content)
        cf_jg = cf_jg.group(1) if cf_jg else ''

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

    def handle_timestmp(self, timestamp):
        """ 处理13位时间戳 """
        timestamps = float(timestamp / 1000)
        time_local = time.localtime(timestamps)
        dt = time.strftime("%Y-%m-%d", time_local)
        return dt