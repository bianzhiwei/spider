# coding=utf-8
# !/usr/bin/python
"""
com_tools
Created on: 2018/8/13  10:25
@author: 卞志伟
"""

import requests
import sys

from requests.packages import urllib3
from selenium import webdriver
import logging
import platform
import string
import zipfile
import time
import datetime
import calendar
import pypinyin
from sqlalchemy import create_engine
from pymysql.err import IntegrityError
import pymysql
import pandas as pd
import records
from lxml import etree
from multiprocessing import Queue

urllib3.disable_warnings()  # 从urllib3中消除警告
platform_ = platform.system()

if platform_ == "Windows":
    chrome_path = 'C:/Python36/chromedriver'
else:
    chrome_path = '/home/youjia/bin/chromedriver'

rank_thread_num = 10  # 爬取排名的线程数量
tag_thread_num = 3  # 爬取标签的线程数量
online_thread_num = 3  # 爬取在线情况的线程数量
calendar_thread_num = 3  # 爬取在线情况的线程数量
hawkeye_thread_num = 15  # 爬取在线情况、日历、标签的线程数量

youjia_13_db = create_engine("mysql+pymysql://username:password@ip/dbname?charset=utf8")
youjia_db = create_engine("mysql+pymysql://username:password@ip/dbname?charset=utf8")

chrom_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 " \
           "(KHTML, like Gecko) Chrome/66.0.3359.139 Safari/537.36"

headers = {"User-Agent": chrom_ua}
# 途家爬取前端时的header
tujia_header = {
    "Connection": "keep-alive",
    "Host": "www.tujia.com",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent": chrom_ua}
# 爱彼迎爬取前端时的header
air_header = {'user-agent': chrom_ua,
              'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
              'accept-encoding': 'gzip, deflate, br',
              'accept-language': 'zh-CN,zh;q=0.9',
              'cache-control': 'max-age=0'}

tj_room_url_tmp = "https://www.tujia.com/detail/%s.htm"
my_room_url_tmp = "http://www.mayi.com/room/%s"
air_room_url_tmp = "https://www.airbnbchina.cn/rooms/%s"

proxies = {
    'http': None,
    'https': None,
}

# 查询蚂蚁、途家、爱彼迎平台上的所有有家房源id及分销平台的房源id
all_room_sql = """SELECT DISTINCT CONCAT(l.id, '') lodge_id, dnsl.third_id room_id,  dnsl.third_type,if( l.state = 0,1,0) state,city.short_name
                    FROM
                        youjia.lodge l INNER JOIN
                        youjia.distribute_need_send_lodge dnsl ON dnsl.true_lodge_id = l.id
                        left join youjia.city on city .id = l.city_id
                    WHERE
                        l.city_id != 376  
                            AND l.state_super = 10
                            AND dnsl.third_type = 1
                            AND dnsl.is_delete = 0
                            AND dnsl.third_id is not null
                            AND dnsl.third_id != ""
                            AND dnsl.third_id != 0
                    GROUP BY l.id            
            UNION
                    SELECT DISTINCT CONCAT(t.id, '') lodge_id, d.listing_id room_id, 20 third_type, if( t.state = 0,1,0) state ,city.short_name
                        FROM youjia.lodge t
                        LEFT JOIN youjia.airbnb_upload_lodge d ON t.id = d.lodge_id
                        LEFT JOIN youjia.airbnb_token ON airbnb_token.work_id = t.work_id
                        LEFT JOIN youjia.city ON city.id = t.id
                        WHERE d.lodge_id IS NOT NULL
                        AND d.listing_id IS NOT NULL
                        AND d.listing_id <> ''
                        AND t.state_super = 10
                        AND t.city_id NOT IN ( 376)
            UNION
                   SELECT DISTINCT CONCAT(b.id, '') lodge_id, a.third_lodge_id room_id, 
                                    a.sales_channel_id third_type ,if( b.state =0,1,0) state ,city.short_name
                        FROM youjia.lod_third_lodge a
                        LEFT JOIN  youjia.lodge b ON a.lodge_id = b.id
                        LEFT JOIN youjia.city ON city.id = b.id
                        WHERE b.city_id NOT IN (376 )
                        AND a.third_lodge_id IS NOT NULL
                        AND a.third_lodge_id <> ''
                        AND b.state_super = 10
                     """
# 查询途家城市id和有家城市id及其城市名称
platfrom_city_sql = """SELECT DISTINCT
                    city.short_name city_name,city.pinyin pinyin, l.city_id yj_city_id, tc.id tj_city_id
                    FROM youjia.lodge l
                    LEFT JOIN youjia.city ON city.id = l.city_id
                    LEFT JOIN youjia_tpp.tujia_city tc ON tc.youjia_id = l.city_id
                    WHERE l.city_id NOT IN (12 , 376) 
                    and l.state_super = 10
                    and l.state = 0
                    """

# 查询途家城市id、商圈id和有家城市id及其城市名称、商圈名称
platfrom_bc_sql = """SELECT distinct city.short_name city,platform_id,city.id youjia_id,hbc.id hbc_id,
                     concat(tbc.bc_id,"") tj_bc_id,concat(tbc.city_id,"") tj_city_id ,hbc.`name` tj_bc_name
                    FROM hawkeye.business_circle hbc
                    LEFT JOIN hawkeye.city ON hbc.city_id = city.id
                    LEFT JOIN youjia_tpp.tujia_city_bc tbc ON tbc.city = city.short_name 
                    and tbc.bc = hbc.`name` and tbc.bc_type = '商圈'
                    ; """


def get_bc_queue():
    bc_queue = Queue()
    bc_df = pd.read_sql_query(platfrom_bc_sql, youjia_13_db)
    for idx, row in bc_df.iterrows():
        bc_queue.put({"city": row.city, "yj_city_id": row.youjia_id, "tj_city_id": row.tj_city_id,
                      "bc": row.tj_bc_name, "hbc_id": row.hbc_id, "platform_id": row.platform_id,
                      "tj_bc_id": row.tj_bc_id, "city_pinyin": pinyin_no(row.city.strip()),
                      "bc_pinyin": pinyin_no(row.tj_bc_name.strip())
                      })
    return bc_queue


def get_city_queue():
    city_queue = Queue()
    city_df = pd.read_sql_query(platfrom_city_sql, youjia_13_db)
    for idx, row in city_df.iterrows():
        city_queue.put({"city": row.city_name, "pinyin": row.pinyin,
                        "yj_city_id": row.yj_city_id, "tj_city_id": row.tj_city_id})
    return city_queue


def get_all_room_queue():
    """
    :return: 获取所有房屋信息  返回一个queue 里面装载的dict
    """
    all_room_queue = Queue()
    all_room_df = pd.read_sql_query(all_room_sql, youjia_db)
    for idx, row in all_room_df.iterrows():
        all_room_queue.put({"lodge_id": row.lodge_id, "room_id": row.room_id,
                            "third_type": row.third_type, "state": row.state, "city": row.short_name})
    return all_room_queue


def get_calendar_room_queue():
    """
    :return: 获取所有房屋信息  返回一个queue 里面装载的dict
    """
    calendar_room_queue = Queue()
    all_room_df = pd.read_sql_query(all_room_sql, youjia_db)
    lodge_ids = all_room_df.lodge_id.unique()
    for lodge_id in lodge_ids:
        lodge_dict = dict()
        third_dict = dict()
        temp_df = all_room_df[all_room_df.lodge_id == lodge_id]
        for idx, row in temp_df.iterrows():
            third_dict[row.third_type] = row.room_id
        lodge_dict[lodge_id] = third_dict
        calendar_room_queue.put(lodge_dict)
    return calendar_room_queue


def get_text_by_xpath(response, xpath):
    """
    通过xpath获取响应的值
    :param response:
    :param xpath:
    :return:    list
    """
    try:
        return etree.HTML(response.text).xpath(xpath)
    except Exception as e:
        print(e)
        return False


def to_day(dft='%Y-%m-%d'):
    """
    :param dft 日期格式
    获取今天日期   默认格式 %Y-%m-%d  可以根据自己想要的格式获取
    %Y-%m-%d %H:%M:%S
    %Y%m%d

    :return:
    """
    return time.strftime(dft, time.localtime())


def get_next_month_today(first_day=datetime.date.today().replace(day=1)):
    """
    获取下月今天的日期
    :param first_day: 默认为本月第一天 %Y-%m-%d 形式字符串
    :return:
    """
    first_day = datetime.datetime.strptime(str(first_day), "%Y-%m-%d").date()
    days_num = calendar.monthrange(first_day.year, first_day.month)[1]  # 获取一个月有多少天
    first_day_of_next_month = first_day + datetime.timedelta(days=days_num)  # 当月的最后一天只需要days_num-1即可
    return str(first_day_of_next_month)


def pinyin_no(word):
    """
    不带声调的(style=pypinyin.NORMAL)
    :param word:  要翻译的汉字
    :return:不带声调的拼音
    """
    s = ''
    for i in pypinyin.pinyin(word, style=pypinyin.NORMAL):
        s += ''.join(i)
    return s


def pinyin_hive(word):
    """
    带声调的(默认)
    :param word: 要翻译的汉字
    :return:
    """
    s = ''
    for i in pypinyin.pinyin(word):
        s = s + ''.join(i) + " "
    return s


def tpp_db():
    return records.Database('mysql+pymysql://username:password@ip/dbname?charset=utf8')


def get_connection():
    """获取youjia_tpp连接 connection"""
    return pymysql.connect(host='ip', user='username', passwd='passwd', db='dbname', port=3306,
                           charset='utf8')


def execute_sql(insert_sql, insert_tuple, update_sql=None, update_tuple=None):
    """
    执行插入sql出现IntegrityError错误时执行update——sql
    :param conn: 数据库链接
    :param insert_sql:
    :param insert_tuple:
    :param update_sql:
    :param update_tuple:
    :return:
    """
    with get_connection() as conn:
        try:
            conn.execute(insert_sql, insert_tuple)
        except IntegrityError:
            conn.execute(update_sql, update_tuple)
        except Exception as msg:
            print(insert_sql)
            print(insert_tuple)
            print(update_sql)
            print(update_tuple)
            logging.exception(msg)


def execute_dict_sql(sql_dict):
    """
    执行插入sql出现IntegrityError错误时执行update——sql
    :param sql_dict:
    :return:
    """
    with get_connection() as conn:
        for insert_sql in sql_dict:
            try:
                insert_list = sql_dict[insert_sql]
                for insert_tuple in insert_list:
                    conn.execute(insert_sql, insert_tuple)
                    # print("执行sql成功")
            except Exception as msg:
                print(insert_sql)
                print(insert_tuple)
                logging.exception(msg)


def conversion_df(df, inv_):
    """
    将DataFramt中的np.nan转化为inv_形式
    :param df:
    :param inv_: 要转化的形式
    :return:
    """
    try:
        return df.astype(object).where(pd.notnull(df), inv_)
    except:
        return df


def is_win():
    """
    判断是否为window系统
    True为window系统  False为Linux系统
    :return: True为window系统  False为Linux系统
    """

    if platform_ == "Windows":
        return True
    return False


def requests_head(url, headers=None, tab=20, time_out=10, mas=None, proxy=proxies):
    """

    :param url:
    :param headers:
    :param tab:
    :param time_out:
    :param sleep_:
    :param mas:
    :param proxy:
    :return:
    """
    if tab == 1:
        logging.exception(mas)
        return False
    try:
        return requests.head(url, headers=headers, proxies=proxy, timeout=time_out, verify=False).status_code
    except Exception as msg:
        return requests_head(url, headers, tab=tab - 1, time_out=10, proxy=proxies, mas=msg)


def requests_get(url=None, headers=headers, data=None, params=None, mas=None, tab=20, time_out=10, sleep_=2,
                 proxy=proxies):
    """
    requests中的get方法
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
        response = requests.get(url=url, headers=headers, data=data, params=params, proxies=proxy, timeout=time_out,
                                verify=False)
        time.sleep(sleep_)
        if response.status_code == 200:
            return response
        return requests_get(url=url, headers=headers, data=data, params=params, tab=tab - 1, time_out=10, mas=mas,
                            proxy=proxy)

    except Exception as e:
        if tab == 1: logging.exception(e)
        return requests_get(url=url, headers=headers, data=data, params=params, tab=tab - 1, time_out=10, mas=e,
                            proxy=proxy)


def requests_post(url=None, headers=headers, data=None, json=None, tab=20, time_out=10, mas=None, sleep_=2,
                  proxy=proxies):
    """
    requests中的post方法
    :param url:
    :param mas:
    :param proxy:
    :param sleep_:
    :param headers:
    :param data:
    :param tab: 访问次数  默认5次不成功返回False
    :param time_out: 设置超时时间
    :return: 返回的是response
    """
    try:
        if tab == 0:
            print("requests_get  访问失败%s次出现错误！！！ " % tab)
            print("错误的url = %s" % url)
            print("错误的信息 : ", mas)
            return False
        response = requests.post(url=url, headers=headers, data=data, json=json, proxies=proxy, timeout=time_out,
                                 verify=False)
        time.sleep(sleep_)
        if response.status_code == 200:
            return response
        return requests_post(url=url, headers=headers, data=data, json=json, tab=tab - 1, time_out=10, mas=mas,
                             proxy=proxy)
    except Exception as e:
        if tab == 1: logging.exception(e)
        return requests_post(url=url, headers=headers, data=data, json=json, tab=tab - 1, time_out=10, mas=e,
                             proxy=proxy)


def create_proxy_auth_extension(scheme='http', plugin_path=None):
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
    proxy_username = "proxy_username"
    proxy_password = "proxy_password"
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


def get_mayi_Headers(cookie_dict=""):
    """
    获取蚂蚁header，cookie摸摸你浏览器
    :param cookie_dict:
    :return:
    """
    mayiheaders = {
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Encoding': 'gzip, deflate',
        'Accept-Language': 'zh-CN,zh;q=0.9',
        'Connection': 'keep-alive',
        'Host': 'www.mayi.com',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'http://www.mayi.com/room/',
        'User-Agent': chrom_ua,
    }
    try:
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument(
            'Accept=text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8')
        chrome_options.add_argument('Accept-Language=gzip, deflate')
        chrome_options.add_argument('Connection=keep-alive')
        chrome_options.add_argument('Host=www.mayi.com')
        chrome_options.add_argument('Upgrade-Insecure-Requests=1')
        chrome_options.add_argument('user-agent=' + chrom_ua)
        driver = webdriver.Chrome(executable_path=chrome_path,
                                  chrome_options=chrome_options)  # executable_path='/usr/bin/google-chrome',
        driver.get("http://www.mayi.com/room")
        time.sleep(10)
        cookies = driver.get_cookies()
        for cookie in cookies:
            cookie_dict += cookie.get('name') + '=' + cookie.get('value') + ';'
        cookie_dict = cookie_dict[:-1]
        mayiheaders['Cookie'] = cookie_dict
    except:
        logging.exception('获取蚂蚁的header出现错误！')
    finally:
        driver.close()
        driver.quit()
    print(mayiheaders)
    return mayiheaders


if __name__ == '__main__':
    # print("是否为window系统：", is_win())
    # url = "https://www.tujia.com/detail/237641.htm"
    # headers = {"User-Agent": chrom_ua}
    # print(requests_get(url=url, headers=headers))
    # print(get_mayi_Headers())
    # x = {"a": 1}
    # print(requests_post(url=url, json=x, data=x))
    a = requests_head("https://www.airbnbchina.cn/rooms/27705036", headers=air_header)
    print(a)
