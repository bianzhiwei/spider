# coding=utf-8
# !/usr/bin/python
"""
tools
Created on: 2018/9/21  14:37
@author: 卞志伟
"""
import config
import requests
import sys
import logging
import string
import zipfile
import time
import datetime
from datetime import timedelta
from multiprocessing import Queue
from pymongo import MongoClient


# conn = MongoClient(config.mongodb_ip, config.mongodb_port)
# db = conn.mydb  #连接mydb数据库，没有则自动创建
# my_set = db.test_set　　#使用test_set集合，没有则自动创建

class Tools(object):
    def __init__(self):
        self.mongo_client_ = None
        self.db = None
        self.city_q = Queue()
        self.conn = None
        # self.init_col()

    def init_conn(self, set_name, base_db="localhome_20180921"):
        if self.mongo_client_ is None:
            self.mongo_client_ = MongoClient(config.mongodb_ip, config.mongodb_port)
        self.db = self.mongo_client_[base_db]  # 连接mydb数据库，没有则自动创建
        return self.db[set_name]  # 使用test_set集合，没有则自动创建

    def today(self, dft='%Y-%m-%d'):
        """
        :param dft 日期格式
        获取今天日期   默认格式 %Y-%m-%d  可以根据自己想要的格式获取
        %Y-%m-%d %H:%M:%S
        %Y%m%d

        :return:
        """
        return time.strftime(dft, time.localtime())

    def tomorrow(self, dft='%Y-%m-%d'):
        """
        :param dft 日期格式
        获取明天日期
        %Y%m%d

        :return:
        """
        return (datetime.datetime.now() + datetime.timedelta(days=1)).strftime(dft)

    def yestoday(self, dft='%Y-%m-%d'):
        """
        :param dft 日期格式
        获取昨天日期
        %Y%m%d

        :return:
        """
        return (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime(dft)

    def request(self, method='get', url=None, headers=config.HEADERS, data=None, params=None, mas=None, tab=20,
                time_out=5,
                sleep_=2, proxy=config.PROXIES):
        """
        requests中的get方法
        :param method:  get ， post ，put , head , options
        :param url:
        :param headers:
        :param data:
        :param params: dict 形式的参数
        :param mas:
        :param tab: 访问次数  默认5次不成功返回False
        :param time_out: 设置超时时间
        :param sleep_:
        :param proxy:
        :return: 返回的是response
        """
        try:
            if tab == 0:
                print("requests_get  访问失败%s次出现错误！！！ " % tab)
                print("错误的url = %s" % url)
                print("错误的信息 : ", mas)
                return False
            response = requests.request(method=method, url=url, headers=headers, data=data, params=params,
                                        proxies=proxy,
                                        timeout=time_out, verify=False)
            time.sleep(sleep_)
            if response.status_code == 200:
                return response
            return self.request(method=method, url=url, headers=headers, data=data, params=params, tab=tab - 1,
                                time_out=10,
                                mas=mas, proxy=proxy)

        except Exception as e:
            if tab == 1: logging.exception(e)
            return self.request(method=method, url=url, headers=headers, data=data, params=params, tab=tab - 1,
                                time_out=10,
                                mas=e,
                                proxy=proxy)

    def create_proxy_auth_extension(self, scheme='http', plugin_path=None):
        """
        给selenium启动的chrome设置代理阿布云代理 方法如下:
        option = webdriver.ChromeOptions() #启动浏览器
        option.add_argument("--start-maximized") # 最大化浏览器
        option.add_extension(proxy_auth_plugin_path) #proxy_auth_plugin_path是这个方法返回的zip路径
        driver = webdriver.Chrome(chrome_options=option) # 添加option

        :param proxy_host:
        :param proxy_port:
        :param proxy_username:
        :param proxy_password:
        :param scheme:
        :param plugin_path:
        :return:
        """
        # 代理服务器
        proxy_host = "http-dyn.abuyun.com"
        proxy_port = "9020"
        # 代理隧道验证信息
        proxy_username = "H4D3E584VRA2842D"
        proxy_password = "7CCD05274100107A"
        if plugin_path is None:
            plugin_path = sys.path[0] + r'/{}_{}@http-dyn.abuyun.com_9020.zip'.format(proxy_username, proxy_password)

        manifest_json = """
                {
                    "version": "1.0.0",
                    "manifest_version": 2,
                    "name": "Abuyun Proxy",
                    "permissions": [
                        "proxy",
                        "tabs",
                        "unlimitedStorage",
                        "storage",
                        "<all_urls>",
                        "webRequest",
                        "webRequestBlocking"
                    ],
                    "background": {
                        "scripts": ["background.js"]
                    },
                    "minimum_chrome_version":"22.0.0"
                }
                """

        background_js = string.Template(
            """
            var config = {
                mode: "fixed_servers",
                rules: {
                    singleProxy: {
                        scheme: "${scheme}",
                        host: "${host}",
                        port: parseInt(${port})
                    },
                    bypassList: ["foobar.com"]
                }
              };
    
            chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});
    
            function callbackFn(details) {
                return {
                    authCredentials: {
                        username: "${username}",
                        password: "${password}"
                    }
                };
            }
    
            chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {urls: ["<all_urls>"]},
                ['blocking']
            );
            """
        ).substitute(
            host=proxy_host,
            port=proxy_port,
            username=proxy_username,
            password=proxy_password,
            scheme=scheme,
        )

        with zipfile.ZipFile(plugin_path, 'w') as zp:
            zp.writestr("manifest.json", manifest_json)
            zp.writestr("background.js", background_js)
        return plugin_path
