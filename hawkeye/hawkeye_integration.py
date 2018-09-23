# coding=utf-8
# !/usr/bin/python
"""
hawk_eye_integration
Created on: 2018/8/28  10:43
@author: 卞志伟
Email:bianzhiwei@iyoujia.com
"""

import datetime
import logging
import time
import com_tools
from lxml import etree
import threading


class RoomDetail(object):
    def __init__(self, lodge_id, third_id, third_type, yj_state, city, my_header):
        self.third_id = third_id
        self.city = city
        self.third_type = third_type
        self.lodge_id = lodge_id
        self.yj_state = yj_state

        self.my_header = my_header
        self.tj_header = com_tools.tujia_header
        self.air_header = com_tools.air_header
        self.headers = com_tools.headers

        self.my_room_url = "https://www.mayi.com/room/%s"
        self.tj_room_url = "https://www.tujia.com/detail/%s.htm"
        self.air_room_url = "https://www.airbnbchina.cn/rooms/%s"
        self.zg_room_url = "https://www.zhenguo.com/housing/%s/"
        self.xz_room_url = "http://xm.xiaozhu.com/fangzi/%s.html"

        self.my_price_url = "http://www.mayi.com/room/month/datePrice"

        self.today = com_tools.to_day('%Y-%m-%d')
        self.two_mon_date = datetime.date.today() + datetime.timedelta(days=60)
        self.start_day = self.first_day_month = datetime.date.today().replace(day=1)
        self.cur_month = datetime.datetime.now().month
        self.cur_year = datetime.datetime.now().year
        self.cur_hour = datetime.datetime.now().hour

        self.crawl_rank = True if self.cur_hour == 22 else False  # 是否爬取排名

        self.response = False
        self.state = 0
        self.srf_token = False
        self.tag_list = list()
        self.calender_list = list()
        self.sql_dict = dict()
        self.calendar_dict = dict()
        self.zg_cookie = self.rf_token = ""

        self.insert_tag = """INSERT INTO `hawkeye`.`lodge_tag` ( `dt`,`lodge_id`,`platform_id`,`tag`)
                VALUES(%s,%s,%s,%s)
                on duplicate key update `platform_id` = %s,`tag` = %s ;
             """
        self.insert_state = """INSERT INTO `hawkeye`.`lodge_online`(`dt`,`platform_id`,`lodge_id`,
                         `youjia_online`,`online`) VALUES( %s,%s,%s,%s,%s)
                        on duplicate key update `youjia_online` = %s,`online` = %s"""

        self.insert_calender = """INSERT INTO `hawkeye`.`calendar` 
                ( `dt`, `platform_id`, `lodge_id`, `available`, `price`) 
                VALUES (%s,%s,%s,%s,%s)
                on duplicate key update `available` = %s,`price` = %s ;"""

    def calender(self):
        if self.state == 1 and self.third_type == 1:
            try:
                params = {"roomid": self.third_id, "startday": self.start_day, "initStock": 1}
                cur_resp = com_tools.requests_get(url=self.my_price_url, headers=self.my_header,
                                                  params=params, proxy=None)
                if cur_resp:
                    for data_day in cur_resp.json()['data']:
                        date = data_day['date']
                        price = data_day['price']
                        vacant_count = data_day['stock']
                        available = data_day['isRent']  # 是否可定  0 不可定  1可定
                        self.calendar_dict[date] = {'status': available, 'price': price, 'vacantCount': vacant_count}
                    if len(self.calendar_dict) < 90:
                        self.start_day = com_tools.get_next_month_today(self.start_day)
                        self.calender()
            except Exception as e:
                logging.exception(e)
        elif self.state == 1 and self.third_type == 3:
            try:
                products = "https://www.tujia.com/bingo/pc/product/getProducts"
                products_data = {"checkInDate": str(self.today), "checkOutDate": str(self.two_mon_date),
                                 "unitId": self.third_id, "activityInfo": None, "callCenter": True}
                products_response = com_tools.requests_post(products, json=products_data, sleep_=0)
                product_id = products_response.json()['data']['products'][0]['productId']
                product_calendar = "https://www.tujia.com/bingo/pc/product/getProductCalendar"
                product_calendar_data = {"productId": product_id, "unitId": self.third_id}
                product_calendar_response = com_tools.requests_post(product_calendar, json=product_calendar_data,
                                                                    sleep_=0)
                date_start = datetime.datetime.strptime(str(self.first_day_month), '%Y-%m-%d')
                for check_day in product_calendar_response.json()['data']['checkIn']:
                    date_str = datetime.datetime.strftime(date_start, '%Y-%m-%d')
                    self.calendar_dict[str(date_str)] = check_day
                    date_start += datetime.timedelta(days=1)
            except Exception as msg:
                logging.exception(msg)
        elif self.state == 1 and self.third_type == 25:
            zg_header = {
                "Cookie": self.zg_cookie,
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0.3440.106 Safari/537.36",
                "x-csrf-token": self.rf_token,
            }
            data = {'productId': 1129482}
            zg_calendar_response = com_tools.requests_post("https://phoenix.meituan.com/gw/cprod/api/v1/calendar/query",
                                                           headers=zg_header, json=data)
            for data in zg_calendar_response.json()['data']['dateInfos']:
                date_str = datetime.datetime.strftime(datetime.datetime.strptime(str(data['date']), '%Y%m%d'),
                                                      '%Y-%m-%d')
                try:
                    price = data['price'] / 100
                except:
                    price = 0
                self.calendar_dict[str(date_str)] = {'status': data['openStatus'],
                                                     'price': price,
                                                     'vacantCount': data['inventoryNum']}

        elif self.state == 1 and self.third_type == 6 and self.srf_token:
            try:
                url = "http://cd.xiaozhu.com/ajax.php"
                headers = {"xSRF-Token": self.srf_token,
                           "Referer": "http://cd.xiaozhu.com/fangzi/%s.html" % self.third_id}
                params = {"op": "AJAX_GetLodgeUnitCalendar", "lodgeunitid": self.third_id,
                          "startdate": "2018-09-01", "enddate": "2018-10-01",
                          "editable": "true", "calendarCode": "true", "rand": "0.3927607474257376"}
                calender_response = com_tools.requests_get(url, headers=headers, params=params, sleep_=0, proxy=None)
                if calender_response:
                    for day_calender in calender_response.json():
                        dt = day_calender['start']
                        available = 1 if day_calender['state'] == 'available' else 0
                        price = day_calender['normalDayPrice']
                        self.calendar_dict[dt] = {'status': available, 'price': price, 'vacantCount': available}
            except Exception as e:
                logging.exception(e)

        elif self.state == 1 and self.third_type == 20:
            params = {"_format": "with_conditions", "count": "3", "listing_id": self.third_id,
                      "month": self.cur_month, "year": self.cur_year, "key": "d306zoyjsyarp7ifhu67rjxn52tv0t20",
                      "currency": "CNY", "locale": "zh"}
            calendar_month = "https://www.airbnbchina.cn/api/v2/calendar_months"
            page_response = com_tools.requests_get(calendar_month, headers=com_tools.air_header, params=params,
                                                   sleep_=0)
            if page_response:
                for calendar_month in page_response.json()['calendar_months']:
                    for day_state in calendar_month['days']:
                        dt = day_state['date']
                        available = 1 if bool(day_state['available']) else 0
                        price = day_state['price']['local_price']
                        self.calendar_dict[dt] = {'status': available, 'price': price, 'vacantCount': available}
        self.storage()

    def tags(self):
        tags_l = list()
        if self.state == 1 and self.third_type == 1:
            try:
                items_tag = etree.HTML(self.response.text).xpath('//*[@id="photo"]')[0]
                text = items_tag.xpath('string(.)').strip()
                youxuan = u'“蚂蚁优选”认证是由蚂蚁短租开创建立的行业新标准，具备3大品质体系、9大服务标准、27项安心保障。住蚂蚁优选房，设施全、服务佳、品质高！' if u'优选' in text else ""
                suding = u'速订' if u'速订房源' in text else ""
                shipai = u'实拍' if u'实拍' in text else ""
                zuofan = u'可做饭' if u'可做饭' in text else ""
                sahnglv = u'商旅' if u'商旅' in text else ""
                changzhu = u'长租优惠' if u'长租优惠' in text else ""
                linha = u'临海房' if u'临海房' in text else ""
                temp_list = [youxuan, suding, shipai, zuofan, sahnglv, changzhu, linha]
                self.tag_list = [elem for elem in temp_list if elem != ""]
            except Exception as e:
                logging.exception(e)

        elif self.state == 1 and self.third_type == 3:
            try:
                pay_tag_xpath = '//*[@id="houseInfo"]/div/div/div[2]/div/div[1]/div/ul/li/span/text()'
                pay_tag = com_tools.get_text_by_xpath(self.response, pay_tag_xpath)
                room_tag_xpath = '//*[@id="tags"]/li/span/text()'
                room_tag = com_tools.get_text_by_xpath(self.response, room_tag_xpath)
                self.tag_list = pay_tag + room_tag
            except Exception as e:
                logging.exception(e)

        elif self.state == 1 and self.third_type == 25:
            try:
                tag_xpath = '//div[@class="room-tag"]/text()'
                self.tag_list = com_tools.get_text_by_xpath(self.response, tag_xpath)
                self.rf_token = etree.HTML(self.response.text).xpath('//meta[@name="csrf-token"]/@content')[0]
                for item in self.response.cookies.items():
                    self.zg_cookie += (item[0] + "=" + item[1] + "; ")
            except Exception as e:
                logging.exception(e)

        # elif self.state == 1 and self.third_type == 6:
        #     try:
        #         tag_xpath = '//div[@class="labels"]/span/text()'
        #         self.tag_list = com_tools.get_text_by_xpath(self.response, tag_xpath)
        #         self.srf_token = etree.HTML(self.response.text).xpath('//input[@id="xz_srf_token"]/@value')[0]
        #     except Exception as e:
        #         logging.exception(e)

        for tag in self.tag_list:
            tag = str(tag).strip()
            if tag != "":
                tags_l.append(tag)
        self.tag_list = tags_l
        # self.storage()
        self.calender()

    def spider(self):
        if self.third_type == 1:
            pass
            url = self.my_room_url % self.third_id
            self.response = com_tools.requests_get(url=url, headers=self.my_header, proxy=None)
            self.state = 0 if not self.response else 1 if u"房间编号" in self.response.text else 0
        elif self.third_type == 3:
            url = self.tj_room_url % self.third_id
            self.response = com_tools.requests_get(url=url, headers=self.tj_header, sleep_=0)
            self.state = 0 if not self.response else 1 if u'房屋描述' in self.response.text else 0
        elif self.third_type == 20:
            url = self.air_room_url % self.third_id
            response_code = com_tools.requests_head(url=url, headers=self.air_header)
            self.state = 0 if not response_code else 1 if response_code == 200 else 0
        elif self.third_type == 25:
            url = self.zg_room_url % self.third_id
            self.response = com_tools.requests_get(url=url, headers=self.headers, sleep_=0)
            self.state = 0 if not self.response else 1 if u'该产品不存在' not in self.response.text else 0
        # elif self.third_type == 6:
        #     url = self.xz_room_url % self.third_id
        #     self.response = com_tools.requests_get(url=url, headers=self.headers, sleep_=0)
        #     self.state = 0 if not self.response else 1 if u'此房间不存在，请重新选择' not in self.response.text else 0

        self.tags()

    def storage(self):
        """
        存储数据库！
        :return:
        """

        state_tuple = (self.today, self.third_type, self.lodge_id, self.yj_state, self.state, self.yj_state, self.state)
        self.sql_dict[self.insert_state] = [state_tuple]
        # print(self.sql_dict)
        tags_list = []
        if isinstance(self.tag_list, list):
            for tag in self.tag_list:
                tag = str(tag).strip()
                tag_tuple = (self.today, self.lodge_id, self.third_type, tag, self.third_type, tag)
                tags_list.append(tag_tuple)
            self.sql_dict[self.insert_tag] = tags_list
        if isinstance(self.calendar_dict, dict):
            for day in self.calendar_dict:
                day_dict = self.calendar_dict[day]
                insert_tuple = (day, self.third_type, self.lodge_id, day_dict['status'],
                                day_dict['price'], day_dict['status'], day_dict['price'])
                self.calender_list.append(insert_tuple)
            self.sql_dict[self.insert_calender] = self.calender_list
        # print(self.sql_dict)

        com_tools.execute_dict_sql(self.sql_dict)


class Start(object):
    def __init__(self):
        """{"lodge_id": row.lodge_id, "room_id": row.room_id,
            "third_type": row.third_type, "state": row.state,
            "city": row.short_name}"""
        self.all_room_queue = com_tools.get_all_room_queue()
        self.my_headers = com_tools.get_mayi_Headers()

    def process(self, third_name):
        print("启动了线程 %s " % third_name)
        while not self.all_room_queue.empty():
            room_dict = self.all_room_queue.get()
            room = RoomDetail(room_dict['lodge_id'], room_dict['room_id'],
                              room_dict['third_type'], room_dict['state'], room_dict['city'], self.my_headers)
            room.spider()
            print(room_dict['third_type'], room_dict['room_id'], room_dict['state'], room.state, room.tag_list,
                  len(room.calendar_dict),
                  room.srf_token)
            # print(room_dict)

    def mul_process(self):
        thread_list = list()
        for i in range(com_tools.hawkeye_thread_num):  # com_tools.online_thread_num):
            third_name = 'LoopThread-%s' % i
            t = threading.Thread(target=self.process, name=third_name, args=(third_name,))
            thread_list.append(t)

            t.start()
            t.join(0.1)
        return thread_list


if __name__ == '__main__':
    print("================================程序启动==============================")
    print("================================%s==============================" % com_tools.to_day('%Y-%m-%d %H:%M:%S'))
    st_time = time.time()
    st = Start()
    thread_list = st.mul_process()
    while len(thread_list) > 0:
        time.sleep(1)
        for thread_ in thread_list:
            if not thread_.is_alive():
                print(thread_.name, '死了！！')
                thread_list.remove(thread_)
    et_time = time.time()
    print("程序结束！共计用时 %s 分钟" % ((et_time - st_time) / 60))
    print("================================程序结束==============================")
    print("================================%s==============================" % com_tools.to_day('%Y-%m-%d %H:%M:%S'))
