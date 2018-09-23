# coding=utf-8
# !/usr/bin/python
"""
spider_localhome
Created on: 2018/9/21  14:36
@author: 卞志伟
"""

from tools import Tools


class LocalHomeDownload(Tools):
    def __init__(self):
        Tools.__init__(self)
        self.url = 'https://ms.localhome.cn/api/base/china-areas?pageSize=500&existCity=true'
        self.citys = None
        self.conn = None
        self.init_spider()
        self.list_url = """https://ms.localhome.cn/api/v2/prod/houses?keyword=%s&pageNumber=%s
        &bedNumberGreaterThanEqual=1&roomNumberGreaterThanEqual=1&tenantNumberGreaterThanEqual=1
        &startDateGreaterThanEqual=%s 00:00:00&endDateLessThan=%s 00:00:00&pageSize=50&pageNum=%s"""
        """
        https://ms.localhome.cn/api/v2/prod/house/984348236332797952/info
        """

    def init_spider(self):
        """
        初始化城市列表每次进来都检查更新最新版
        :return:
        """

        self.conn = self.init_conn("city")
        self.citys = self.conn.find_one()
        # print(self.citys)
        if self.citys is None:
            response = self.request(url=self.url)
            # self.conn.insert(response.json())
            self.conn.update({'name': "city"}, response.json())
            self.citys = self.conn.find_one()
        self.conn = self.init_conn("util_list")
        print(r'初始化城市列表完毕！ 需要爬取的城市如下')
        for city in self.citys['data']['list']:
            self.city_q.put(city['name'])
            print(city['name'], end=',')

    def start_spider(self):
        while not self.city_q.empty():
            city = self.city_q.get()
            self.list_spider(city)

    def list_spider(self, city, page_num=1):
        url = self.list_url % (city, page_num, self.today(), self.tomorrow(), page_num)
        print(url)
        list_response = self.request(url=url)
        if list_response:
            response_json = list_response.json()
            for unit_json in response_json['data']['list']:
                unit_json['unit_id'] = unit_json['id']
                self.conn.insert(unit_json)
            if response_json['data']['size'] == 50:
                self.list_spider(city=city, page_num=page_num + 1)


class LocalHomeAnalysis(object):
    def __init__(self):
        pass


if __name__ == '__main__':
    local_home = LocalHomeDownload()
    local_home.start_spider()
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
