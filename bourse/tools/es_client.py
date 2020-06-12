# -*- coding: utf-8 -*-
import elasticsearch
from elasticsearch import helpers
from elasticsearch.helpers import bulk


class ESClient(object):

    # def __init__(self, host="114.115.129.113", port=9999, index_name=None, index_type=None):
    def __init__(self, host_port, index_name=None, index_type=None):
        self.index_name = index_name
        self.index_type = index_type
        self.es = elasticsearch.Elasticsearch(host_port, timeout=3600)

    def insert_data(self, item, _id):
        result = self.es.create(index=self.index_name, doc_type=self.index_type, id=_id, body=item)
        return result

    def update_data(self, item, _id):
        item = {'doc': item}
        result = self.es.update(index=self.index_name, doc_type=self.index_type, id=_id, body=item)
        return result

    def update_by_query(self, query):
        result = self.es.update_by_query(index=self.index_name, doc_type=self.index_type, body=query)
        return result

    # 单条查询
    def search_by_query(self, query):
        result = self.es.search(index=self.index_name, body=query)
        return result

    def search_all(self, item):
        result = self.es.search(index=self.index_name, body=item)
        return result

    def get_data_by_id(self, _id):
        result = self.es.get(index=self.index_name, doc_type=self.index_type, id=_id, ignore=[404])
        return result

    def add_data(self, row_obj):
        """
        单条插入ES
        """
        _id = row_obj.get("_id", 1)
        row_obj.pop("_id")
        self.es.index(index=self.index_name, doc_type=self.index_type, body=row_obj, id=_id)

    def add_data_bulk(self, row_obj_list):
        """
        批量插入ES
        """
        load_data = []
        i = 1
        bulk_num = 20000  # 2000条为一批
        for row_obj in row_obj_list:
            action = {
                "_index": self.index_name,
                "_type": self.index_type,
                "_id": row_obj.get('_id', 'None'),
                "_source": {
                    'oname': row_obj.get('oname', None),
                    'uccode': row_obj.get('uccode', None),
                    'cf_sy': row_obj.get('cf_sy', None),
                    'cf_jg': row_obj.get('cf_jg', None),
                    'cf_cflb': row_obj.get('cf_cflb', None),
                    'cf_jdrq': row_obj.get('cf_jdrq', None),
                    'cf_wsh': row_obj.get('cf_wsh', None),
                    'cf_xzjg': row_obj.get('cf_xzjg', None),
                    'sj_type': '67',
                    "site_id": 20906,
                    "xxly": '中华人民共和国-失信联合惩戒名单',
                    "cf_type": '失信联合惩戒',
                }
            }
            load_data.append(action)
            i += 1
            # 批量处理
            if len(load_data) == bulk_num:
                print('批量插入')
                success, failed = bulk(self.es, load_data, index=self.index_name, raise_on_error=True)
                del load_data[0:len(load_data)]
                print(success, failed)
                # print("一次共插入:%s条数据" % (len(load_data)))

        if len(load_data) > 0:
            success, failed = bulk(self.es, load_data, index=self.index_name, raise_on_error=True)
            print("插入成功")
            del load_data[0:len(load_data)]
            print(success, failed)

    def get_es_id(self):
        """
        获取es唯一id
        :return:
        """
        query = {
            "query": {"match": {
                "oname": "深圳"
            }},
            "size": 20
        }
        result = self.search_by_query(query)
        hits_data = result.get("hits").get("hits")
        for items in hits_data:
            _id = items.get("_id")
            yield _id

    def scroll_search(self, query_body, scroll='10m'):
        """
        游标遍历es库，返回的是生成器类型
        :param query_body: 查询数据语句
        :return: 查询的生成器
        """
        es_result = helpers.scan(
            client=self.es,
            query=query_body,
            scroll=scroll,
            index=self.index_name,
        )
        return es_result

    def delete_by_id(self, _id):
        """通过住建id删除文档"""
        self.es.delete(index=self.index_name, doc_type=self.index_type, id=_id)

    def delete_by_query(self, query_body):
        """删除搜索的数据"""
        self.es.delete_by_query(index=self.index_name, body=query_body)


if __name__ == '__main__':
    ES_HOSTNAME = "49.4.22.216"
    ES_TCP_PORT = 9999

    host_port = [ES_HOSTNAME + ":" + str(ES_TCP_PORT)]
    index_name = "cf_index_db"
    index_type = "xzcf"

    es_client = ESClient(host_port, index_name=index_name, index_type=index_type)

    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"xxly.keyword": "江苏省-徐州市生态环境局-行政处罚"}},
                    {"term": {"sj_ztxx": "0"}},
                ],
                "must_not": [],
                "should": []
            }
        },
        "from": 0,
        "size": 10,
        "sort": [],
        "aggs": {}
    }
    datas = es_client.scroll_search(query_body)

    for idx, item in enumerate(datas):
        if idx < 100:
            print(item)
        else:
            break

