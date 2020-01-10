# -*- coding: utf-8 -*-
from fake_useragent import UserAgent


class RandomUserAgentMiddleware(object):
    """
    利用fake_useragent生成随机请求头
    """
    def __init__(self, ua_type):
        self.ua_type = ua_type
        self.ua = UserAgent()

    @classmethod
    def from_crawler(cls, crawler):
        return cls(
            ua_type=crawler.settings.get('RANDOM_UA_TYPE', 'random')
        )

    def process_request(self, request, spider):
        def get_user_agent():
            return getattr(self.ua, self.ua_type)
        request.headers.setdefault(b'User-Agent', get_user_agent())