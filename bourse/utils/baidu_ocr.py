# -*- coding: utf-8 -*-
import os

import jsonpath
from aip import AipOcr


def parse_img_pdf(images):
    """ 调用百度识别接口-识别无法复制的PDF文件-必须传入byte字节，否则无法识别 """
    """ 你的 APPID AK SK """
    APP_ID = '14964808'
    API_KEY = 'AVWLHd7wAOxf4kijuImGZzVH'
    SECRET_KEY = 'SEZTAAYH92VFTFXEvc75Vyi4nROfE0I0'

    options = {}
    options["detect_direction"] = "true"
    options["probability"] = "true"

    client = AipOcr(APP_ID, API_KEY, SECRET_KEY)
    # 高精度识别
    content = client.basicAccurate(images, options)
    # print(content)
    return content


def parse_url_pdf(url):

    APP_ID = '14964808'
    API_KEY = 'AVWLHd7wAOxf4kijuImGZzVH'
    SECRET_KEY = 'SEZTAAYH92VFTFXEvc75Vyi4nROfE0I0'

    options = {}
    options["language_type"] = "CHN_ENG"
    options["detect_direction"] = "true"
    options["detect_language"] = "true"
    options["probability"] = "true"

    client = AipOcr(APP_ID, API_KEY, SECRET_KEY)
    # 通用文字识别
    content = client.basicGeneralUrl(url, options)
    print(content)


def get_content(image_path):
    """获取图片里面的文字，返回字符串"""
    image_paths = os.walk(image_path)
    for parents, dirnames, filenames in image_paths:
        for filename in filenames:
            image_path = os.path.join(parents, filename)

            with open(image_path, 'rb') as f:
                images = f.read()
            json_results = parse_img_pdf(images)
            words = jsonpath.jsonpath(json_results, "$.words_result[*].words")
            result = ''.join(words)
            print(result)


if __name__ == '__main__':
    path = './image'
    get_content(path)
    # url = "https://timgsa.baidu.com/timg?image&quality=80&size=b9999_10000&sec=1578472068106&di=c6a8cacbbeb71b324ad5ed6663d08be5&imgtype=0&src=http%3A%2F%2Fb-ssl.duitang.com%2Fuploads%2Fitem%2F201710%2F29%2F20171029112734_s8mXF.jpeg"
    # parse_url_pdf(url)