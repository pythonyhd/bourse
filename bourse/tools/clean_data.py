# -*- coding: utf-8 -*-
import copy
import time
import jpype

from bourse import settings
from bourse.tools.es_client import ESClient


class CleanData(object):

    def __init__(self):
        # 采集ES数据库
        self.es_client = ESClient(settings.ES_HOST_NAME, index_name=settings.ES_INDEX_NAME,
                                  index_type=settings.ES_INDEX_TYPE)
        # 清洗ES数据库
        # self.es_client = ESClient(settings.QX_ES_HOST, index_name=settings.QX_ES_INDEX_NAME,
        #                       index_type=settings.QX_ES_INDEX_TYPE)

        # 如果是测试，则不修改es数据库
        self.is_test = True
        # self.is_test = False

        # 是否删除，物理删除
        # self.is_delete = True
        self.is_delete = False

        # 是否清洗数据，如果清洗数据则过清洗数据规则，否则只更新数据状态
        self.is_qx_sj = False
        # self.is_qx_sj = True

    def run(self):
        """主程序"""
        jclass = self.get_jclass()  # 获取java类的实例

        data_iter, total_count = self.query_data()
        num = 1
        for idx, data in enumerate(data_iter):
            item = data.get("_source")
            org_item = copy.deepcopy(item)  # 保存原始数据
            modify_item = self.modify_data(item)
            # 判断修改字典，如果为None，则跳过不修改
            if not modify_item:
                continue

            item = {**org_item, **modify_item}

            print("current nums --> {}/{}".format(num, total_count))
            num += 1
            if not self.is_test:
                if self.is_qx_sj:
                    # 调用java接口，插入es中
                    item = self.dict2jmap(item)     # python中的dict对象转化为java中map类型
                    arrayList = jpype.java.util.ArrayList()
                    arrayList.add(item)
                    jclass.data2es_gx(arrayList)
                    # 调用java接口结束
                else:
                    self.update_data(item)
            else:
                if idx == 1000:
                    break

    def query_data(self):
        """
        根据条件筛选站点，数据
        sj_bs_bj: 意义：sj_bs_bj枚举值
        0	正常
        1	‘依据 事由结果’ 都为空
        2	‘处罚日期发布日期‘ 都为空
        3	地区名称 为空
        4	文书号长度超过50
        5	主体长度超过60
        6	机关长度超过50
        7	法人长度超过12
        8	主体名称包含错误词
        9	法人名称包含错误词
        10	文书号包含错误词
        11	机关包含错误词
        12	事由 结果 都为空
        13	处罚日期大于当前日期
        14	处罚日期为空
        15	机关名称为空
        16	主体名称包含‘码‘
        17	主体名称‘关于，将‘开始的
        18	文书号为空
        19	本地运行的采集
        20	本地文件上传
        """
        query_body = {
            "query": {
                "bool": {
                    "must": [
                        {"term": {"xxly.keyword": "浙江省法院公开网-限制招投标"}},
                        {"term": {"sj_ztxx": 0}},  # 信息状态，0：未入库，1: 入库，-1:删除, -2
                        # {"term": {"sj_type": 9}},  # 数据类型 9:税务处罚， 10：重大涉税 4:股权冻结
                        # {"term": {"sj_bs_bj": 17}},  # 14: 处罚时间为空  15:处罚机关为空
                        # {"term": {"cf_xzjg": "国家税务总局"}},
                        # {"term": {"xq_url": "http://121.18.64.252/xzzfgs/xsqcon-988889006-168615-644.html"}},  # 字段包含词
                        # {"query_string": {"query": "\"2020-04-15\"", "default_field": "cf_jdrq"}},  # 字段中包含query词
                        # {"query_string": {"query": "\"(或单位)\"", "default_field": "oname"}},  # 字段中包含query词
                    ],
                    "must_not": [
                        {"term": {"sj_ztxx": -1}},  # 信息状态，0：未入库，1: 入库，-1:删除
                        # {"term": {"sj_ztxx": 1}},  # 信息状态，0：未入库，1: 入库，-1:删除
                        # {"query_string": {"query": "\"\"", "default_field": "cf_cfmc.keyword"}},  # 字段中包含query词
                        # {"term": {"cf_cfmc.keyword": ""}}
                        # {"exists": {"field": "cf_wsh"}}    # 查找字段名值为空的
                        # {"query_string": {"query": "\"元\"", "default_field": "cf_jg"}},  # 字段中包含query词
                        # {"query_string": {"query": "\"罚款\"", "default_field": "cf_jg"}},  # 字段中包含query词
                    ],
                    "should": [
                        # {"term": {"cf_wsh": ""}}
                    ]
                },
                # "regexp": {
                #     "cf_xzjg": ".*\d{4}年\d{1,2}月\d{1,2}日",
                # },
            },
            "from": 0,
            "size": 10,
            "sort": [
                {"cf_jdrq": "desc"}
            ],
            "aggs": {}
        }

        data = self.es_client.search_by_query(query_body)
        print("总数量为: ", data.get("hits", {}).get("total"))

        data_iter = self.es_client.scroll_search(query_body)
        return data_iter, data.get("hits", {}).get("total")

    def modify_data(self, item: dict):
        """处理数据, 将需要处理的字段放到新的字典info中, 之后会更新到原始的数据中"""
        # 是否清洗数据，如果清洗数据则过清洗数据规则，否则只更新数据状态, 默认是false
        # self.is_qx_sj = True
        # self.is_test = False  # 是否是测试, 不测试, 默认是True

        info = {
            "oname": item.get("oname", ""),  # 主体名称
            "cf_cfmc": item.get("cf_cfmc"),
            "cf_jdrq": item.get("cf_jdrq"),
            "cf_wsh": item.get("cf_wsh"),
            "cf_xzjg": item.get("cf_xzjg"),
            # "cf_r_id": item.get("cf_r_id"),
            # "cf_r_dz": item.get("cf_r_dz"),
            # "zxfy": item.get("zxfy"),
            # "wbbz": item.get("wbbz"),
            # "cf_type": item.get("cf_type"),
            # "cf_cflb": item.get("cf_cflb"),
            # "sj_bs_bj": item.get("sj_bs_bj"),
            "cf_sy": item.get("cf_sy"),
            "cf_jg": item.get("cf_jg"),
            # "pname": item.get("pname"),
            # "ws_nr_txt": item.get("ws_nr_txt"),
            # "wbbz": item.get("wbbz"),
            # "cf_level": item.get("cf_level"),
            # "dq_mc": item.get("dq_mc"),
            "xxly": item.get("xxly"),
            # "bz": item.get("bz"),
            "xq_url": item.get("xq_url"),
            "sj_type": item.get("sj_type"),   # 数据类型
            "cj_sj": item.get("cj_sj"),
            # "sj_type": 7,
            # "ws_pc_id": item.get("ws_pc_id", ""),  # 排重id
            # "xg_sj": item.get("xg_sj"),
            "xg_sj": int(time.time()),  # 修改时间，修改的数据要加上这个
            # "sj_ztxx": item.get("sj_ztxx"),
            "sj_ztxx": 1,  # 数据状态信息，0：未入库，1:入库，-1已删除
            # "sj_xz": 1,  # 处罚时间使用发布日期
            # "cj_sj": int(time.time()),
            # "cj_sj": item.get("cj_sj")
        }

        # ######### 修改字段--开始
        # ######### 修改字段--开始


        # ####### 修改字段结束
        # # ####### 修改字段结束

        print(info)
        return info

    def update_data(self, item):
        """更新es中的数据"""
        _id = item.get("ws_pc_id")
        if not _id:
            return

        # 更新es中的数据
        self.es_client.update_data(item, _id)

    def get_jclass(self):
        """开启jvm，调用java类，创建类实例"""
        jpype.startJVM(settings.jvmPath, "-ea", settings.jclasspath, settings.jdependency, convertStrings=False)    # 开启java虚拟机
        java_api = jpype.JClass(settings.jclassName)    # 调用jar的类
        jcleaner = java_api()   # 实例化类

        return jcleaner

    def dict2jmap(self, item):
        """
        python中的dict对象转化为java中的map对象
        :param item:
        :return:
        """
        hashMap = jpype.java.util.HashMap()
        for key, value in item.items():
            hashMap.put(key, value)
        return hashMap


if __name__ == '__main__':
    cleaner = CleanData()
    cleaner.run()
    # data_iter = cleaner.query_data()
    # cleaner.handle_data(data_iter)