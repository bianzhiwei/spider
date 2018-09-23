# coding=utf-8
# !/usr/bin/python
"""
platform_ranking
Created on: 2018/8/14  10:16
@author: 卞志伟
"""
import com_tools
import pandas as pd
import datetime
import threading
import time
import logging
from lxml import etree

global my_headers  # 声明一个蚂蚁header的全局变量

today = str(datetime.date.today())
bc_queue = com_tools.get_bc_queue()
city_queue = com_tools.get_city_queue()

all_room_df = pd.read_sql_query(com_tools.all_room_sql, com_tools.youjia_13_db)


def ari_rank(rank_place, page_index=0, rank_list=list(), crawl_num=330, rank=0):
    """
    爬取爱彼迎排名  无论城市还是商圈都可以用这个
    :param rank_place: 爬虫地点
    :param page_index: 以第一页开始  第一页为0
    :param rank_list: rankList为存储排名的房源id的list
    :param crawl_num: 设置爬取的条数
    :param rank:
    :return:
    """
    if page_index == 0:
        rank_list = list()
    ari_rank_url = "https://www.airbnbchina.cn/api/v2/explore_tabs"
    headers = {"User-Agent": com_tools.chrom_ua}
    params = {
        "section_offset": page_index,  # 设置这是第几页  0开始
        "_format": "for_explore_search_web",
        "items_per_grid": "20",  # 设置一页的大小
        "selected_tab_id": "home_tab",
        "query": rank_place,
        "key": "d306zoyjsyarp7ifhu67rjxn52tv0t20",
    }
    response = com_tools.requests_get(ari_rank_url, headers=headers, params=params, sleep_=0)
    if response:
        remark_ids = response.json()['explore_tabs'][0]['home_tab_metadata']['remarketing_ids']
        listings_count = response.json()['explore_tabs'][0]['home_tab_metadata']['listings_count']
        if listings_count < crawl_num:
            crawl_num = listings_count
        if len(remark_ids) == 0:
            # print(len(rank_list), " 爱彼迎最终排名抓取的条数")
            return rank_list
        for unit_id in remark_ids:
            rank += 1
            rank_list.append([str(unit_id), rank])
        if len(rank_list) < crawl_num:
            # print(len(rank_list), end=' ')
            return ari_rank(rank_place, page_index=page_index + 1, rank_list=rank_list, crawl_num=crawl_num, rank=rank)
        else:
            # print(len(rank_list), " 爱彼迎最终排名抓取的条数")
            return rank_list


def my_rank(place_pinyin, page_index=1, rank_list=list(), crawl_num=330, rank=0, my_headers=None):
    """
    爬取蚂蚁排名  无论城市还是商圈都可以用这个
    :param place_pinyin: 要爬取的城市商圈的拼音
    :param page_index: 以第一页开始  第一页为1
    :param rank_list: rankList为存储排名的房源id的list
    :param crawl_num: 设置爬取的条数
    :param rank:
    :param my_headers:
    :return:
    """
    if page_index == 1:
        rank_list = list()
    my_list_url = 'http://www.mayi.com/%s/%s/?map=no' % (place_pinyin, page_index)
    response = com_tools.requests_post(my_list_url, headers=my_headers, proxy=None, sleep_=0)
    if response:
        html = etree.HTML(response.text)
        pg_active = html.xpath('//*[@id="page"]/a[@class="pg-active"]/text()')
        if pg_active != [str(page_index)]:
            # print(len(rank_list), " 是蚂蚁最终排名抓取的条数   ")
            return rank_list
        for dd in (html.xpath('//*[@id="searchRoom"]/dd')):
            # print(dd.xpath("./@data")[0])
            rank += 1
            rank_list.append([str(dd.xpath("./@data")[0]), rank])
        if len(rank_list) < crawl_num:
            # print(len(rank_list), end=' ')
            return my_rank(place_pinyin, page_index=page_index + 1, rank_list=rank_list, crawl_num=crawl_num,
                           rank=rank, my_headers=my_headers)
        else:
            # print(len(rank_list), " 是蚂蚁最终排名抓取的条数    ")
            return rank_list


def tj_city_rank(city_id, page_index=0, rank_list=list(), crawl_num=330, rank=0):
    """
    途家城市排名
    :param city_id: 途家的城市的id
    :param page_index: 以第一页开始  第一页为0
    :param rank_list: rankList为存储排名的房源id的list
    :param crawl_num: 设置爬取的条数
    :param rank: 排名
    :return: 爬取300条记录或者没有记录之后的rankList
    """
    if page_index == 0:
        rank_list = list()
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    date_value = str(today) + "," + str(yesterday)
    search_url = 'https://client.tujia.com/tmsv4/searchunitfull'
    headers = {"User-Agent": com_tools.chrom_ua}
    json_data = {"bi": "{\"picturemode\":\"small\"}",
                 "parameter": {"H5Url": None, "VillaChannelThemeId": 0, "conditions":
                     [{"gType": 0, "isHotRecommend": False, "isLandmark": False, "label": None, "sentValue": None,
                       "type": 47,
                       "value": date_value}, {"gType": 1, "isHotRecommend": False, "isLandmark": False, "label": "1人",
                                              "sentValue": None, "type": 8, "value": "1"},
                      {"gType": 0, "isHotRecommend": False, "isLandmark": False, "label": None,
                       "sentValue": None, "type": 42, "value": city_id}], "onlyNeedUnitCount": False,
                               "pageIndex": page_index, "pageSize": 20,
                               "returnAllConditions": True, "returnNavigations": True},
                 "client": {"appId": "com.tujia.hotel",
                            "appVersion": "6.60_75", "channelCode": "", "devModel": "", "devToken": "", "devType": 2,
                            "locale": "zh-CN",
                            "osVersion": "", "screenInfo": "", "uID": ""}, "code": None, "psid": "", "type": "",
                 "user": None, "usid": None}
    resp = com_tools.requests_post(search_url, headers=headers, json=json_data, sleep_=0)
    if resp:
        resp_json = resp.json()
        unit_list = resp_json['content']['list']
        if len(unit_list) == 0:
            # print(len(rank_list), " 途家城市最终排名抓取的条数")
            return rank_list
        for util in unit_list:
            unit_id = util['unitId']
            rank += 1
            rank_list.append([str(unit_id), rank])
        if len(rank_list) < crawl_num:
            # print(len(rank_list), end=' ')
            return tj_city_rank(city_id, page_index + 1, rank_list=rank_list, crawl_num=crawl_num, rank=rank)
        else:
            # print(len(rank_list), " 途家城市最终排名抓取的条数")
            return rank_list


def tj_bc_rank(city, city_id, bc, bc_id, page_index=1, rank_list=list(), crawl_num=330, rank=0):
    """
             商圈排名
    :param city: 途家的城市的名
    :param city_id: 途家的城市的id
    :param bc: 途家的商圈名
    :param bc_id: 途家的商圈id
    :param page_index: 以第一页开始  第一页为1
    :param rank_list: rankList为存储排名的房源id的list
    :param crawl_num: 爬取多少个
    :param rank: 排名
    :return: 爬取300条记录或者没有记录之后的rankList
    """
    if page_index == 1:
        rank_list = list()
    today = datetime.date.today()
    yesterday = today - datetime.timedelta(days=1)
    dateValue = str(today) + "," + str(yesterday)
    searchUrl = 'https://client.tujia.com/tmsv4/searchunitfull'
    headers = {
        "User-Agent": "Mozilla/5.0 (Linux; Android 5.1.1; vivo X7 Build/LMY47V) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Version/4.0 Chrome/39.0.0.0 Mobile Safari/"
                      "537.36 tujia(hotel/6.60/75 mNet/wifi loc/zh_CN)",
    }
    json_data = {"bi": "{\"picturemode\":\"small\"}",
                 "parameter": {"H5Url": None, "VillaChannelThemeId": 0, "conditions":
                     [{"gType": 1, "isHotRecommend": False, "isLandmark": False, "label": "1人", "sentValue": None,
                       "type": 8, "value": "1"},
                      {"gType": 0, "isHotRecommend": False, "isLandmark": False, "label": city, "sentValue": None,
                       "type": 42, "value": city_id},
                      {"gType": 2, "isHotRecommend": False, "isLandmark": False, "label": bc, "sentValue": None,
                       "type": 11, "value": bc_id},
                      {"gType": 1, "isHotRecommend": False, "isLandmark": False, "label": "5公里以内", "sentValue": None,
                       "type": 10, "value": "5000"},
                      {"gType": 4, "isHotRecommend": False, "isLandmark": False, "label": "推荐排序", "sentValue": None,
                       "type": 48, "value": "1"},
                      {"gType": 0, "isHotRecommend": False, "isLandmark": False, "label": "", "sentValue": None,
                       "type": 47, "value": dateValue}],
                               "onlyNeedUnitCount": False, "pageIndex": page_index, "pageSize": 20,
                               "returnAllConditions": False, "returnNavigations": False},
                 "client": {"appId": "com.tujia.hotel", "appVersion": "6.60_75", "channelCode": "", "devModel": "",
                            "devToken": "", "devType": 2, "locale": "zh-CN", "osVersion": "", "screenInfo": "",
                            "uID": ""},
                 "code": None, "psid": "", "type": "searchunitfull", "user": None, "usid": None}
    resp = com_tools.requests_post(searchUrl, headers=headers, json=json_data, sleep_=0)
    if resp:
        resp_json = resp.json()

        try:
            unitlist = resp_json['content']['list']
        except:
            # print("途家商圈爬虫出现错误！", city, city_id, bc, bc_id, resp_json)
            unitlist = []

        if len(unitlist) == 0:
            # print(len(rank_list), " 途家商圈%s_%s最终排名抓取的条数" % (city, bc))
            return rank_list
        for util in unitlist:
            rank += 1
            rank_list.append([str(util['unitId']), rank])
        if len(rank_list) < crawl_num:
            # print(len(rank_list), end=' ')
            return tj_bc_rank(city, city_id, bc, bc_id, page_index + 1, rank_list=rank_list, crawl_num=crawl_num,
                              rank=rank)
        else:
            # print(len(rank_list), " 途家商圈%s_%s最终排名抓取的条数" % (city, bc))
            return rank_list


def judge_lodge_id(df_row, business_circle_id, yj_city_id):
    """
    用来判断lodge_id是否为空返回相应的sql和相应的 tuple
    :param df_row:
    :param business_circle_id:
    :param yj_city_id:
    :return:
    """
    if df_row.lodge_id == "" or str(df_row.lodge_id) == '0' or df_row.lodge_id is None:
        insert_hawkeye_city = """INSERT INTO `hawkeye`.`city_ranking` (`dt`,`platform_id`,
                             `third_id`,`city_id`,`ranking`) VALUES (%s,%s,%s,%s,%s);
                            """
        insert_hawkeye_bc = """INSERT INTO `hawkeye`.`business_circle_ranking`
                                    ( `dt`,`platform_id`,`third_id`,`business_circle_id`,`ranking`,`create_date`)
                                    VALUES(%s,%s,%s,%s,%s,%s);
                                """
        city_tuple = (today, str(df_row.third_type), str(df_row.room_id), yj_city_id, str(df_row['rank']))
        bc_tuple = (today, df_row.third_type, df_row.room_id, business_circle_id, df_row['rank'], today)
    else:
        insert_hawkeye_city = """INSERT INTO `hawkeye`.`city_ranking` (`dt`,`platform_id`,`lodge_id`,
                             `third_id`,`city_id`,`ranking`) VALUES (%s,%s,%s,%s,%s,%s);
                            """
        insert_hawkeye_bc = """INSERT INTO `hawkeye`.`business_circle_ranking`
                                ( `dt`,`platform_id`,`lodge_id`,`third_id`,`business_circle_id`,`ranking`,`create_date`)
                                    VALUES(%s,%s,%s,%s,%s,%s,%s);
                                """
        city_tuple = (today, df_row.third_type, df_row.lodge_id, df_row.room_id, yj_city_id, df_row['rank'])
        bc_tuple = (
            today, df_row.third_type, df_row.lodge_id, df_row.room_id, business_circle_id, df_row['rank'], today)

    if business_circle_id is None:
        return insert_hawkeye_city, city_tuple
    else:
        return insert_hawkeye_bc, bc_tuple


def storage_city_db(rank_list, yj_city_id, third_type, business_circle_id=None):
    """
    :param rank_list: 存储排名的id
    :param yj_city_id:
    :param third_type: 分销平台
    :param business_circle_id: 商圈id
    :return:
    """
    rank_df = pd.merge(pd.DataFrame(rank_list, columns=['room_id', 'rank']), all_room_df, how='left', on=['room_id'])
    rank_df['third_type'] = rank_df.apply(lambda row: third_type, axis=1)
    rank_df = com_tools.conversion_df(rank_df, None)
    for idx, row in rank_df.iterrows():
        if idx % 100 == 0 and idx > 99:
            print(today, third_type, row.lodge_id, row.room_id, yj_city_id, business_circle_id, "存储完成了多少个：", idx)
        insert_sql, data_tuple = judge_lodge_id(row, business_circle_id, yj_city_id)
        com_tools.execute_sql(insert_sql, data_tuple)


def process_city():
    """
    执行爬取城市方法
    :return:
    """
    global my_headers
    while not city_queue.empty():
        city_dict = city_queue.get()
        try:
            tj_rank_list = tj_city_rank(city_dict['tj_city_id'])
            storage_city_db(tj_rank_list, city_dict['yj_city_id'], 3)
            print("途家城市： %s 排名存储完成！" % city_dict['city'], len(tj_rank_list))
            ari_rank_list = ari_rank(city_dict['city'])
            storage_city_db(ari_rank_list, city_dict['yj_city_id'], 20)
            print("爱彼迎城市： %s 排名存储完成！" % city_dict['city'], len(ari_rank_list))
            my_rank_list = my_rank(city_dict['pinyin'], my_headers=my_headers)
            storage_city_db(my_rank_list, city_dict['yj_city_id'], 1)
            print("蚂蚁城市： %s 排名存储完成！" % city_dict['city'], len(my_rank_list))
        except Exception as e:
            logging.exception(e)



def process_bc():
    """
    {"city": row.city, "yj_city_id": row.youjia_id, "tj_city_id": row.tj_city_id,
    "bc": row.tj_bc_name, "tj_bc_id": row.tj_bc_id, "city_pinyin": com_tools.pinyin_no(row.city.strip()),
    "bc_pinyin": com_tools.pinyin_no(row.tj_bc_name.strip())
                  }
    :return:
    """
    global my_headers
    while not bc_queue.empty():
        bc_dict = bc_queue.get()
        try:
            platform_id = bc_dict['platform_id']
            if platform_id == 3:
                tj_rank_list = tj_bc_rank(bc_dict['city'], bc_dict['tj_city_id'], bc_dict['bc'], bc_dict['tj_bc_id'])
                storage_city_db(tj_rank_list, bc_dict['yj_city_id'], platform_id, bc_dict['hbc_id'])
                print("途家商圈： %s_%s 排名存储完成！" % (bc_dict['city'], bc_dict['bc']), len(tj_rank_list))
            elif platform_id == 20:
                air_rank_list = ari_rank(bc_dict['bc'])
                storage_city_db(air_rank_list, bc_dict['yj_city_id'], platform_id, bc_dict['hbc_id'])
                print("爱彼迎商圈： %s_%s 排名存储完成！" % (bc_dict['city'], bc_dict['bc']), len(air_rank_list))
            elif platform_id == 1:
                my_rank_list = my_rank(bc_dict['city_pinyin'] + "_" + bc_dict['bc_pinyin'], my_headers=my_headers)
                storage_city_db(my_rank_list, bc_dict['yj_city_id'], platform_id, bc_dict['hbc_id'])
                print("蚂蚁商圈： %s_%s 排名存储完成！" % (bc_dict['city'], bc_dict['bc']), len(my_rank_list))

        except Exception as e:
            logging.exception(e)


def crawl_city():
    """
    爬取城市
    :return:
    """
    thread_list = list()
    for i in range(com_tools.rank_thread_num):
        t = threading.Thread(target=process_city, name='LoopThread-%s' % i)
        thread_list.append(t)
        t.start()
        t.join(0.1)


def crawl_bc():
    """
    爬取商圈
    :return:
    """
    thread_list = list()
    for i in range(com_tools.rank_thread_num):
        t = threading.Thread(target=process_bc, name='LoopThread-%s' % i)
        thread_list.append(t)
        t.start()
        t.join(0.1)


def look_my_header():
    """
    监控蚂蚁的header   半小时更新一次
    :return:
    """
    while True:
        time.sleep(60 * 30)
        global my_headers
        my_headers = com_tools.get_mayi_Headers()


def start():
    global my_headers
    my_headers = com_tools.get_mayi_Headers()

    # t = threading.Thread(target=look_my_header, name='look_my_header')
    # t.start()
    # t.join(0.1)

    crawl_city()
    crawl_bc()


if __name__ == '__main__':
    start()
