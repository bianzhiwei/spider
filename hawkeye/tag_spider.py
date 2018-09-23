# coding=utf-8
# !/usr/bin/python
"""
tag_spider
Created on: 2018/8/20  14:35
@author: 卞志伟
"""
import com_tools
from lxml import etree
import datetime
import threading

today = datetime.date.today()

global my_headers
all_room_queue = com_tools.get_all_room_queue()

insert_tag = """INSERT INTO `hawkeye`.`lodge_tag` ( `dt`,`lodge_id`,`platform_id`,`tag`)
                VALUES(%s,%s,%s,%s)
                on duplicate key update `platform_id` = %s,`tag` = %s 
                ;
             """


def tj_room_tags(room_id):
    """
    获取途家所有标签
    :param room_id:
    :return:
    """
    response = com_tools.requests_get(com_tools.tj_room_url_tmp % room_id, headers=com_tools.tujia_eader, sleep_=0)
    if response and u'房屋描述' in response.text:
        try:
            pay_tag_xpath = '//*[@id="houseInfo"]/div/div/div[2]/div/div[1]/div/ul/li/span/text()'
            pay_tag = com_tools.get_text_by_xpath(response, pay_tag_xpath)
            room_tag_xpath = '//*[@id="tags"]/li/span/text()'
            room_tag = com_tools.get_text_by_xpath(response, room_tag_xpath)
            return pay_tag + room_tag
        except:
            return []


def my_room_tags(room_id):
    """
    获取蚂蚁所有标签
    :param room_id:
    :return:
    """
    global my_headers
    my_room_url = com_tools.my_room_url_tmp % room_id
    response = com_tools.requests_get(my_room_url, headers=my_headers, sleep_=0, proxy=None)
    if response and u"房间编号" in response.text:
        # pay_tag_xpath = '//div[@class="biaoqian"]/span/text()'
        # pay_tag = com_tools.get_text_by_xpath(response, pay_tag_xpath)
        items_tag = etree.HTML(response.text).xpath('//*[@id="photo"]')[0]
        text = items_tag.xpath('string(.)').strip()
        youxuan = u'“蚂蚁优选”认证是由蚂蚁短租开创建立的行业新标准，具备3大品质体系、9大服务标准、27项安心保障。住蚂蚁优选房，设施全、服务佳、品质高！' if u'优选' in text else ""
        suding = u'速订' if u'速订房源' in text else ""
        shipai = u'实拍' if u'实拍' in text else ""
        zuofan = u'可做饭' if u'可做饭' in text else ""
        sahnglv = u'商旅' if u'商旅' in text else ""
        changzhu = u'长租优惠' if u'长租优惠' in text else ""
        linha = u'临海房' if u'临海房' in text else ""
        temp_list = [youxuan, suding, shipai, zuofan, sahnglv, changzhu, linha]
        tag_list = [elem for elem in temp_list if elem != ""]
        return tag_list
    return []


def storage_tag_db(lodge_id, room_id, third_type, room_tags):
    """`dt`,`lodge_id`,`platform_id`,`tag`"""
    if isinstance(room_tags, dict):
        for tag in room_tags:
            data_tuple = (today, lodge_id, third_type, tag, third_type, tag)
            com_tools.execute_sql(insert_tag, data_tuple)


def process():
    """
    lodge_id": row.lodge_id, "room_id": row.room_id, "third_type":
    :return:
    """
    while not all_room_queue.empty():
        room_dict = all_room_queue.get()
        lodge_id = room_dict['lodge_id']
        room_id = room_dict['room_id']
        third_type = room_dict['third_type']
        if third_type == 3:
            storage_tag_db(lodge_id, room_id, third_type, tj_room_tags(room_id))
            print(lodge_id, 3)
        elif third_type == 1:
            storage_tag_db(lodge_id, room_id, third_type, my_room_tags(room_id))
            print(lodge_id, 1)
        elif third_type == 20:
            pass


def main():
    global my_headers
    my_headers = com_tools.get_mayi_Headers()
    for i in range(com_tools.tag_thread_num):
        t = threading.Thread(target=process, name='look_my_header')
        t.start()
        t.join(0.1)


if __name__ == '__main__':
    main()
