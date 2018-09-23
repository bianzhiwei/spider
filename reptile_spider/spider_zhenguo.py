# coding=utf-8
# !/usr/bin/python
"""
spider_localhome
Created on: 2018/9/21  14:36
@author: 卞志伟
"""

import re
import sys
import json
import config
import threading
from tools import Tools
from multiprocessing import Queue
from pymongo.errors import DuplicateKeyError


class ZhenGuoDownload(Tools):
    def __init__(self):
        Tools.__init__(self)
        self.start_url = 'https://www.zhenguo.com/'
        self.locations = "https://www.zhenguo.com/api/phx/cprod/locations?cityId=%s"
        self.list_url = "https://www.zhenguo.com/api/phx/cprod/products?dateBegin=%s" \
                        "&dateEnd=%s&cityPinyin=%s&locationCategory=%s&locationId=%s&pageNow=%s"
        self.conn = None
        self.citys = None
        self.search_unit = None
        self.zhenguo_city_q = Queue()
        self.init_spider()

    def init_spider(self):
        """
        初始化城市列表每次进来都检查更新最新版
        :return:
        """
        print("------" * 10, "榛果爬虫正在加载 城市，请稍等。。。", "------" * 10)
        self.conn = self.init_conn(set_name="city", base_db=config.ZHENGUO_DB)
        self.citys = self.conn.find_one()
        print(self.conn.database)
        if self.citys is None:
            response = self.request(url=self.start_url)
            city_list = re.search('{"cities":(.*),"defaultConditions":', response.text).group(1)
            self.citys = {'_id': 'city', 'name': 'city', 'citys': json.loads(city_list)}
            self.conn.insert(self.citys)
            print("------" * 10, __file__, sys._getframe().f_lineno, "榛果城市列表更新完毕", "------" * 10)
        print("------" * 10, __file__, sys._getframe().f_lineno, "榛果爬虫正在加载 商圈 ，请稍等。。。", "------" * 10)
        self.conn = self.init_conn(set_name="search_unit", base_db=config.ZHENGUO_DB)
        self.search_unit = self.conn.find_one()
        for city in self.citys['citys']:
            unit_infos = self.conn.find_one(filter={"_id": city['id']})
            if unit_infos is None:
                response = self.request(url=self.locations % city['id'])
                unit_infos = response.json()
                unit_infos['_id'] = city['id']
                unit_infos.update(city)
                self.conn.insert(unit_infos)
                print("------" * 10, __file__, sys._getframe().f_lineno, "榛果城市", city['nm'], "下商圈列表更新完毕", "------" * 10)
            listings = unit_infos['data']['listings']
            for listing in listings:
                for unit_info in listings[listing]:
                    unit_info.update({"locationCategory": listing, "cityPinyin": city['pynm'],
                                      "city": city['nm'], "city_id": city['id']})
                    self.zhenguo_city_q.put(unit_info)
                    print("------" * 10, __file__, sys._getframe().f_lineno, "榛果", unit_info['city'], "商圈",
                          unit_info['name'], "加载完毕", "------" * 10)
            # print(self.zhenguo_city_q.get())
        self.conn = self.init_conn(set_name="util_list", base_db=config.ZHENGUO_DB)

    def list_spider(self, unit_info, page_num=1):
        url = self.list_url % (self.today('%Y%m%d'), self.tomorrow('%Y%m%d'), unit_info['cityPinyin'],
                               unit_info['locationCategory'], unit_info['id'], page_num)
        print(url)
        list_response = self.request(url=url)
        if list_response:
            response_json = list_response.json()
            for unit_json in response_json['data']['list']:
                unit_json['unit_id'] = unit_json['productId']
                unit_json['_id'] = unit_json['productId']
                unit_json['locationId'] = unit_info['id']
                unit_json.update(unit_info)
                unit_json.pop('id')
                try:
                    self.conn.insert(unit_json)
                except DuplicateKeyError:
                    pass
                except Exception as msg:
                    print(msg)
            if len(response_json['data']['list']) == response_json['data']['pageSize'] :
                self.list_spider(unit_info=unit_info, page_num=page_num + 1)

    def start_spider(self, third_name):
        print("------" * 10, __file__, sys._getframe().f_lineno, "线程", third_name, "开始了", "------" * 10)
        while not self.zhenguo_city_q.empty():
            unit_info = self.zhenguo_city_q.get()
            # print(unit_info)
            self.list_spider(unit_info)

    def run(self):
        for i in range(config.THREAD_SPIDER_NUM):  # com_tools.online_thread_num):
            third_name = 'LoopThread-%s' % i
            t = threading.Thread(target=self.start_spider, name=third_name, args=(third_name,))
            t.start()
            t.join(0.1)


class LocalHomeAnalysis(object):
    def __init__(self):
        pass


if __name__ == '__main__':
    local_home = ZhenGuoDownload()
    local_home.run()
    # import requests
    #
    # PROXIES = {
    #     'http': 'http://H4D3E584VRA2842D:7CCD05274100107A@http-dyn.abuyun.com:9020',
    #     'https': 'http://H4D3E584VRA2842D:7CCD05274100107A@http-dyn.abuyun.com:9020',
    # }
    # resp = requests.request(method='get', verify=False,
    #                         url="https://ms.localhome.cn/api/base/china-areas?pageSize=500&existCity=true",
    #                         proxies=PROXIES)
    # print(resp.text)
