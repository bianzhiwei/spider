# coding=utf-8
# !/usr/bin/python
"""
calendar_spider
Created on: 2018/8/21  18:55
@author: 卞志伟
Email:bianzhiwei@iyoujia.com
"""

import com_tools
from lxml import etree
import datetime
import threading
import time
import logging

cur_month = datetime.datetime.now().month
cur_year = datetime.datetime.now().year
first_day_month = datetime.date.today().replace(day=1)
today = datetime.date.today()
two_mon_date = today + datetime.timedelta(days=60)
calendar_room_queue = com_tools.get_calendar_room_queue()
global my_headers
insert_sql = """INSERT INTO `hawkeye`.`calendar` 
                ( `dt`, `platform_id`, `lodge_id`, `available`, `price`) 
                VALUES (%s,%s,%s,%s,%s)
                on duplicate key update `available` = %s,`price` = %s ;
                """

update_sql = """UPDATE `hawkeye`.`calendar`
                SET `available` = %s,`price` = %s
                WHERE `id` = %s;
             """


def tj_calendar(room_id):
    calendar_dict = dict()
    page_response = com_tools.requests_get(com_tools.tj_room_url_tmp % room_id, headers=com_tools.tujia_header, sleep_=0)
    if page_response and u'房屋描述' in page_response.text:
        try:
            products = "https://www.tujia.com/bingo/pc/product/getProducts"
            products_data = {"checkInDate": str(today), "checkOutDate": str(two_mon_date), "unitId": room_id,
                             "activityInfo": None, "callCenter": True}
            products_response = com_tools.requests_post(products, json=products_data)
            product_id = products_response.json()['data']['products'][0]['productId']
            product_calendar = "https://www.tujia.com/bingo/pc/product/getProductCalendar"
            product_calendar_data = {"productId": product_id, "unitId": room_id}
            product_calendar_response = com_tools.requests_post(product_calendar, json=product_calendar_data)
            date_start = datetime.datetime.strptime(str(first_day_month), '%Y-%m-%d')
            for check_day in product_calendar_response.json()['data']['checkIn']:
                date_str = datetime.datetime.strftime(date_start, '%Y-%m-%d')
                calendar_dict[str(date_str)] = check_day
                date_start += datetime.timedelta(days=1)
        except Exception as msg:
            logging.error(msg)
    return calendar_dict


def air_calendar(room_id):
    """
    cur_month = datetime.datetime.now().month
    cur_year = datetime.datetime.now().year
    :param room_id:
    :return:
    """
    calendar_dict = dict()
    params = {"_format": "with_conditions",
              "count": "3",
              "listing_id": room_id,
              "month": cur_month,
              "year": cur_year,
              "key": "d306zoyjsyarp7ifhu67rjxn52tv0t20",
              "currency": "CNY",
              "locale": "zh"}
    calendar_month = "https://www.airbnbchina.cn/api/v2/calendar_months"
    url = com_tools.air_room_url_tmp % room_id
    cur_response = com_tools.requests_get(url=url, headers=com_tools.air_header)

    if cur_response.url == url:
        page_response = com_tools.requests_get(calendar_month, headers=com_tools.air_header, params=params, sleep_=0)
        if page_response:
            for calendar_month in page_response.json()['calendar_months']:
                for day_state in calendar_month['days']:
                    dt = day_state['date']
                    available = 1 if bool(day_state['available']) else 0
                    price = day_state['price']['local_price']
                    calendar_dict[dt] = {'status': available, 'price': price, 'vacantCount': available}

    return calendar_dict


def my_calendar(room_id, start_day=first_day_month, calendar_dict=dict()):
    global my_headers
    if start_day == first_day_month:
        calendar_dict = dict()
    date_price_url = "http://www.mayi.com/room/month/datePrice"
    params = {"roomid": room_id, "startday": start_day, "initStock": 1}
    cur_response = com_tools.requests_get(url=date_price_url, headers=my_headers, params=params, proxy=None)
    if cur_response:
        for data_day in cur_response.json()['data']:
            date = data_day['date']
            price = data_day['price']
            vacant_count = data_day['stock']
            available = data_day['isRent']  # 是否可定  0 不可定  1可定
            calendar_dict[date] = {'status': available, 'price': price, 'vacantCount': vacant_count}
        if len(calendar_dict) < 90:
            return my_calendar(room_id, start_day=com_tools.get_next_month_today(start_day),
                               calendar_dict=calendar_dict)
        else:
            return calendar_dict
    else:
        return calendar_dict


def storage_tag_db(lodge_id, room_id, third_type, calendar_dict):
    """
    :param lodge_id:
    :param room_id:
    :param third_type:
    :param calendar_dict:
    :return:
    """
    for day_ in calendar_dict:
        # str_time = time.strftime("%Y-%m-%d", time.strptime(str(day_), "%Y-%m-%d %H:%M:%S"))
        time_id = day_.replace("-", "")
        temp_id = str(room_id) + str(third_type) + str(time_id)
        day_dict = calendar_dict[day_]
        status = day_dict['status']  # 是否可定  0 不可定  1可定
        price = day_dict['price']  # 价格
        vacant_count = day_dict['vacantCount']  # 剩余可定
        insert_tuple = (day_, third_type, lodge_id, status, price, status, price)
        # update_tuple = (status, price, temp_id)
        # print(insert_sql % insert_tuple)
        com_tools.execute_sql(insert_sql, insert_tuple)  # , update_sql, update_tuple)


def start():
    """
    {'184298317981693675': {1: '852754615', 20: '25325681', 3: '2463825'}}
    :return:
    """
    while not calendar_room_queue.empty():
        lodge_dict = calendar_room_queue.get()
        for lodge_id in lodge_dict:
            room_dict = lodge_dict[lodge_id]
            for third_type in room_dict:
                room_id = room_dict[third_type]
                print(lodge_id, third_type, room_id)
                if third_type == 3:
                    tj_calendar_dict = tj_calendar(room_id)
                    storage_tag_db(lodge_id, room_id, third_type, tj_calendar_dict)
                elif third_type == 20:
                    air_calendar_dict = air_calendar(room_id)
                    storage_tag_db(lodge_id, room_id, third_type, air_calendar_dict)
                elif third_type == 1:
                    my_calendar_dict = my_calendar(room_id)
                    storage_tag_db(lodge_id, room_id, third_type, my_calendar_dict)


def look_my_header():
    """
    监控蚂蚁的header  半小时更新一次
    :return:
    """
    for i in range(8):
        time.sleep(60 * 30)
        global my_headers
        my_headers = com_tools.get_mayi_Headers()


def thread_start():
    # t = threading.Thread(target=look_my_header, name='look_my_header')
    # t.start()
    # t.join(0.1)

    for i in range(com_tools.calendar_thread_num):
        t = threading.Thread(target=start, name='look_my_header')
        t.start()
        t.join(0.1)
    # look_my_header()


def main():
    global my_headers
    my_headers = com_tools.get_mayi_Headers()

    st = time.time()
    thread_start()
    ed = time.time()
    print(ed - st)


if __name__ == '__main__':
    main()
