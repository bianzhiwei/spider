# coding=utf-8
# !/usr/bin/python
"""
config
Created on: 2018/9/20  16:04
@author: 卞志伟
Email:bianzhiwei@iyoujia.com
"""
import platform

THREAD_SPIDER_NUM = 6  # 爬虫线程
THREAD_ANALYSIS_NUM = 3  # 解析线程

PROXIES = {
    'http': 'http://H4D3E584VRA2842D:7CCD05274100107A@http-dyn.abuyun.com:9020',
    'https': 'http://H4D3E584VRA2842D:7CCD05274100107A@http-dyn.abuyun.com:9020',
}

ZHENGUO_DB = 'zhenguo_20180922'

UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
     "(KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"

HEADERS = {"User-Agent": UA}

mongodb_url_ = 'mongodb://192.168.80.23:27017/'

mongodb_ip = '192.168.80.23'
mongodb_port = 27017

platform_ = platform.system()
if platform_ == "Windows":
    is_win = True
    chrome_path = 'C:/Python36/chromedriver'
else:
    is_win = False
    chrome_path = '/home/youjia/bin/chromedriver'
