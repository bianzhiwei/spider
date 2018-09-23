# coding=utf-8
# !/usr/bin/python
"""
online_monitoring
Created on: 2018/8/13  14:29
@author: 卞志伟
Email:bianzhiwei@iyoujia.com
"""
import pandas as pd
import time, datetime
import com_tools
from multiprocessing import Queue
import threading

today_ = datetime.date.today()
today = str(today_).replace("-", "")
room_queue = Queue()

tujia_headers = com_tools.tujia_header
air_header = com_tools.air_header
MaYiRoomUrl = "http://www.mayi.com/room/%s"
TuJiaRoomUrl = "https://www.tujia.com/detail/%s.htm"
AirbnbRoomUrl = "https://www.airbnbchina.cn/rooms/%s"
mayi_h_l = list()


def set_room_queue():
    all_room_df = pd.read_sql_query(com_tools.all_room_sql, com_tools.youjia_db)
    for idx, room_row in all_room_df.iterrows():
        room_queue.put(list(room_row))


# def process():


def process(mayi_Headers):
    """状态：0已上线，10已下线',"""
    # with get_connection as conn:
    while not room_queue.empty():
        row_list = room_queue.get()
        lodge_id = row_list[0]
        third_id = row_list[1]
        third_type = row_list[2]
        youjia_state = 1 if row_list[3] == 0 else 0
        if third_type == 1:
            url = MaYiRoomUrl % third_id
            response = com_tools.requests_get(url=url, headers=mayi_Headers, proxy=None)
            state = 0 if not response else 1 if u"房间编号" in response.text else 0
        elif third_type == 3:
            url = TuJiaRoomUrl % third_id
            response = com_tools.requests_get(url=url, headers=tujia_headers)
            state = 0 if not response else 1 if u'房屋描述' in response.text else 0
        elif third_type == 20:
            url = AirbnbRoomUrl % third_id
            response_code = com_tools.requests_head(url=url, headers=air_header)
            state = 0 if not response else 1 if response_code == 200 else 0

        # 0 不在线 1 在线',
        print(lodge_id, third_id, third_type, state, url)
        l_online_id = str(third_id) + str(today)
        insert_sql = """INSERT INTO `hawkeye`.`lodge_online`(`dt`,`platform_id`,`lodge_id`,
                         `youjia_online`,`online`) VALUES( %s,%s,%s,%s,%s)
                        on duplicate key update `youjia_online` = %s,`online` = %s"""
        insert_tuple = (today_, third_type, lodge_id, youjia_state, state, youjia_state, state)
        # update_sql = "UPDATE `hawkeye`.`lodge_online` SET youjia_online = %s , `online` = %s WHERE `id` = %s;"
        # update_tuple = (youjia_state, state, l_online_id)
        # print(insert_sql % insert_tuple)
        com_tools.execute_sql(insert_sql, insert_tuple)


def start(mayi_Headers):
    print('程序开始执行...')
    thread_list = list()
    for i in range(com_tools.online_thread_num):
        t = threading.Thread(target=process, name='LoopThread-%s' % i, args=(mayi_Headers,))
        thread_list.append(t)
        t.start()
        t.join(0.1)


# def run():
#     is_first = True
#     while True:
#         print("----")
#         if room_queue.empty():
#             if not is_first:
#                 time.sleep(30 * 60)
#             is_first = False
#             set_room_queue()
#             mayi_Headers = com_tools.get_mayi_Headers()
#             start(mayi_Headers)
#         time.sleep(50)


def main():
    # while True:
    #     try:
    #         run()
    #     except Exception as e:
    #         print(e)
    set_room_queue()
    mayi_Headers = com_tools.get_mayi_Headers()
    time.sleep(60)
    start(mayi_Headers)


if __name__ == '__main__':
    main()
