# -*- coding: UTF-8 -*-

"""

@Project Name: als_upload_for_it
@File Name:    als_upload_for_it

@User:         smile
@Author:       Smile
@Email:        Xiaofei.Smile365@Gmail.com

@Date Time:    2021/2/18 10:31
@IDE:          PyCharm

@程式功能简介：
此程式用于对ALS产生的csv格式的log进行清洗转换为xml，并上抛到IT的ftp服务器；
1. 监控ALS生成log的文件夹，使用看门狗watchdog的形式
2. 将csv格式转换为xml格式
3. 将xml上抛到it给定的ftp服务器

"""


import os

import time
import datetime

from watchdog.observers import Observer
from watchdog.events import *

import csv

from xml.dom.minidom import Document

from ftplib import *

import uuid
import socket


def write_run_record(message_text):
    """对程式的运行及文件处理过程写入日志文件，已备后续查询"""
    # 检测日志文件夹是否存在
    if not os.path.exists("./record/"):
        os.makedirs("./record/")

    # 获取日志路径&需写入的信息
    message = message_text

    # 打开日志并写入
    with open(f"./record/{str(datetime.datetime.now().strftime('%Y_%m_%d'))}.txt", 'a') as file_record:
        file_record.write(f"{message}\n")


def csv_to_xml(file_path, xml_file_path):
    """将csv格式转为xml格式，并转存到xml文件夹"""
    # 关键节点写入日志
    print(f"{datetime.datetime.now()}: 读取csv文件")
    write_run_record(f"{datetime.datetime.now()}: 读取csv文件")

    # 打开csv文件并读取数据为list
    with open(file_path, 'r') as file:
        reader = csv.reader(file)
        data_list = list(reader)

    # 关键节点写入日志
    print(f"{datetime.datetime.now()}: 文件数据为：{data_list}")
    write_run_record(f"{datetime.datetime.now()}: 文件数据为：{data_list}")

    # 获取相应数据并赋值给相应变量
    result = str(str(str(file_path).split('/')[2]).split('.')[0]).split('_')[0]  # 测试结果 from name
    sn = str(str(str(file_path).split('/')[2]).split('.')[0]).split('_')[1]  # 产品sn from name

    ng_code = data_list[1][0]  # ng code from log
    lux_hex_0 = data_list[1][1]  # 测试数据 Lux_Hex[0] from log
    lux_hex_1 = data_list[1][2]  # 测试数据 Lux_Hex[1] from log
    lux_hex_2 = data_list[1][3]  # 测试数据 Lux_Hex[2] from log
    device_id = data_list[1][4]  # 测试数据 Device_ID from log
    lux_dark_1 = data_list[1][5]  # 测试数据 LUX dark1 from log
    test_site = data_list[1][6]  # 测试站点 from log
    lux_dark_2 = data_list[1][7]  # 测试数据 LUX dark2 from log

    # 可能获取不到时间，索引错误
    try:
        test_time = data_list[1][8]  # 测试时间 from log;可能获取不到时间，故放在预留栏位前
    except IndexError:
        test_time = "Data_is_Null"

    lux_bright = data_list[3][0]  # 测试结果的数值 from log
    lux_dark = data_list[3][1]  # 测试数据LUX_dark from log

    # 预留七个数据栏位，可能获取不到数据，索引错误
    try:
        reserved_field_1 = data_list[3][2]  # 预留栏位1的数值 from log;可能获取不到数据
        reserved_field_2 = data_list[3][3]  # 预留栏位2的数值 from log;可能获取不到数据
        reserved_field_3 = data_list[3][4]  # 预留栏位3的数值 from log;可能获取不到数据
        reserved_field_4 = data_list[3][5]  # 预留栏位4的数值 from log;可能获取不到数据
        reserved_field_5 = data_list[3][6]  # 预留栏位5的数值 from log;可能获取不到数据
        reserved_field_6 = data_list[3][7]  # 预留栏位6的数值 from log;可能获取不到数据
        reserved_field_7 = data_list[3][8]  # 预留栏位7的数值 from log;可能获取不到数据
    except IndexError:
        reserved_field_1 = "Data_is_Null"  # 预留栏位1的数值 from log;可能获取不到数据
        reserved_field_2 = "Data_is_Null"  # 预留栏位2的数值 from log;可能获取不到数据
        reserved_field_3 = "Data_is_Null"  # 预留栏位3的数值 from log;可能获取不到数据
        reserved_field_4 = "Data_is_Null"  # 预留栏位4的数值 from log;可能获取不到数据
        reserved_field_5 = "Data_is_Null"  # 预留栏位5的数值 from log;可能获取不到数据
        reserved_field_6 = "Data_is_Null"  # 预留栏位6的数值 from log;可能获取不到数据
        reserved_field_7 = "Data_is_Null"  # 预留栏位7的数值 from log;可能获取不到数据

    # 获取本机计算机名、用户名、IP地址、MAC地址
    pc_name = socket.gethostname()
    pc_ip = socket.gethostbyname(pc_name)

    # 获取MAC地址
    def get_mac_address():
        """获取本机MAC地址"""
        mac = uuid.UUID(int=uuid.getnode()).hex[-12:]
        return ":".join([mac[e:e + 2] for e in range(0, 11, 2)])

    pc_mac = get_mac_address()

    # 关键节点写入日志
    print(f"{datetime.datetime.now()}: 写入xml文件")
    write_run_record(f"{datetime.datetime.now()}: 写入xml文件")

    # 生成xml文件
    doc = Document()
    order_pack = doc.createElement("ALS_Log_Data")
    doc.appendChild(order_pack)
    object_name = "Data"

    objectE = doc.createElement(object_name)
    objectE.setAttribute("Data", "Data_List")

    object_pc_name = doc.createElement("PC_Name")
    object_pc_name_text = doc.createTextNode(pc_name)
    object_pc_name.appendChild(object_pc_name_text)
    objectE.appendChild(object_pc_name)

    object_pc_ip = doc.createElement("PC_IP")
    object_pc_ip_text = doc.createTextNode(pc_ip)
    object_pc_ip.appendChild(object_pc_ip_text)
    objectE.appendChild(object_pc_ip)

    object_pc_mac = doc.createElement("PC_MAC")
    object_pc_mac_text = doc.createTextNode(pc_mac)
    object_pc_mac.appendChild(object_pc_mac_text)
    objectE.appendChild(object_pc_mac)

    object_result = doc.createElement("Result")
    object_result_text = doc.createTextNode(result)
    object_result.appendChild(object_result_text)
    objectE.appendChild(object_result)

    object_sn = doc.createElement("SN")
    object_sn_text = doc.createTextNode(sn)
    object_sn.appendChild(object_sn_text)
    objectE.appendChild(object_sn)

    object_ng_code = doc.createElement("NG_Code")
    object_ng_code_text = doc.createTextNode(ng_code)
    object_ng_code.appendChild(object_ng_code_text)
    objectE.appendChild(object_ng_code)

    object_lux_hex_0 = doc.createElement("Lux_Hex_0")
    object_lux_hex_0_text = doc.createTextNode(lux_hex_0)
    object_lux_hex_0.appendChild(object_lux_hex_0_text)
    objectE.appendChild(object_lux_hex_0)

    object_lux_hex_1 = doc.createElement("Lux_Hex_1")
    object_lux_hex_1_text = doc.createTextNode(lux_hex_1)
    object_lux_hex_1.appendChild(object_lux_hex_1_text)
    objectE.appendChild(object_lux_hex_1)

    object_lux_hex_2 = doc.createElement("Lux_Hex_2")
    object_lux_hex_2_text = doc.createTextNode(lux_hex_2)
    object_lux_hex_2.appendChild(object_lux_hex_2_text)
    objectE.appendChild(object_lux_hex_2)

    object_device_id = doc.createElement("Device_ID")
    object_device_id_text = doc.createTextNode(device_id)
    object_device_id.appendChild(object_device_id_text)
    objectE.appendChild(object_device_id)

    object_lux_dark_1 = doc.createElement("LUX_dark1")
    object_lux_dark_1_text = doc.createTextNode(lux_dark_1)
    object_lux_dark_1.appendChild(object_lux_dark_1_text)
    objectE.appendChild(object_lux_dark_1)

    object_test_site = doc.createElement("Test_Site")
    object_test_site_text = doc.createTextNode(test_site)
    object_test_site.appendChild(object_test_site_text)
    objectE.appendChild(object_test_site)

    object_lux_dark_2 = doc.createElement("LUX_dark2")
    object_lux_dark_2_text = doc.createTextNode(lux_dark_2)
    object_lux_dark_2.appendChild(object_lux_dark_2_text)
    objectE.appendChild(object_lux_dark_2)

    object_test_time = doc.createElement("Test_Time")
    object_test_time_text = doc.createTextNode(test_time)
    object_test_time.appendChild(object_test_time_text)
    objectE.appendChild(object_test_time)

    object_lux_bright = doc.createElement("LUX_bright")
    object_lux_bright_text = doc.createTextNode(lux_bright)
    object_lux_bright.appendChild(object_lux_bright_text)
    objectE.appendChild(object_lux_bright)

    object_lux_dark = doc.createElement("LUX_dark")
    object_lux_dark_text = doc.createTextNode(lux_dark)
    object_lux_dark.appendChild(object_lux_dark_text)
    objectE.appendChild(object_lux_dark)

    object_reserved_field_1 = doc.createElement("Reserved_field_1")
    object_reserved_field_1_text = doc.createTextNode(reserved_field_1)
    object_reserved_field_1.appendChild(object_reserved_field_1_text)
    objectE.appendChild(object_reserved_field_1)

    object_reserved_field_2 = doc.createElement("Reserved_field_2")
    object_reserved_field_2_text = doc.createTextNode(reserved_field_2)
    object_reserved_field_2.appendChild(object_reserved_field_2_text)
    objectE.appendChild(object_reserved_field_2)

    object_reserved_field_3 = doc.createElement("Reserved_field_3")
    object_reserved_field_3_text = doc.createTextNode(reserved_field_3)
    object_reserved_field_3.appendChild(object_reserved_field_3_text)
    objectE.appendChild(object_reserved_field_3)

    object_reserved_field_4 = doc.createElement("Reserved_field_4")
    object_reserved_field_4_text = doc.createTextNode(reserved_field_4)
    object_reserved_field_4.appendChild(object_reserved_field_4_text)
    objectE.appendChild(object_reserved_field_4)

    object_reserved_field_5 = doc.createElement("Reserved_field_5")
    object_reserved_field_5_text = doc.createTextNode(reserved_field_5)
    object_reserved_field_5.appendChild(object_reserved_field_5_text)
    objectE.appendChild(object_reserved_field_5)

    object_reserved_field_6 = doc.createElement("Reserved_field_6")
    object_reserved_field_6_text = doc.createTextNode(reserved_field_6)
    object_reserved_field_6.appendChild(object_reserved_field_6_text)
    objectE.appendChild(object_reserved_field_6)

    object_reserved_field_7 = doc.createElement("Reserved_field_7")
    object_reserved_field_7_text = doc.createTextNode(reserved_field_7)
    object_reserved_field_7.appendChild(object_reserved_field_7_text)
    objectE.appendChild(object_reserved_field_7)

    order_pack.appendChild(objectE)

    # 写入到xml文件
    xml_file = f"{xml_file_path}{result}_{sn}.xml"
    with open(xml_file, 'w') as file:
        doc.writexml(file, indent='\t', newl='\n', addindent='\t', encoding='gbk')

    # 关键节点写入日志
    print(f"{datetime.datetime.now()}: 完成数据写入：【{xml_file}】")
    write_run_record(f"{datetime.datetime.now()}: 完成数据写入：【{xml_file}】")

    return os.path.abspath(xml_file)


def upload_xml_file(xml_file, created_file_name):
    """将xml文件上传到ftp服务器"""
    def ftp_connect(host, username, password):
        """打开ftp服务器"""
        ftp = FTP()
        ftp.connect(host=host)
        ftp.login(username, password)
        ftp.cwd("/xml/")
        return ftp

    # 关键节点写入日志
    print(f"{datetime.datetime.now()}: 打开ftp服务器")
    write_run_record(f"{datetime.datetime.now()}: 打开ftp服务器")

    # 打开ftp服务器
    ftp_server = ftp_connect("10.5.19.66", "smile", "5210")
    buf_size = 1024

    # 关键节点写入日志
    print(f"{datetime.datetime.now()}: 打开本地xml文件")
    write_run_record(f"{datetime.datetime.now()}: 打开本地xml文件")

    # 打开本地xml文件
    fp = open(xml_file, 'rb')
    remote_file_name = created_file_name.split('.')[0] + ".xml"

    # 关键节点写入日志
    print(f"{datetime.datetime.now()}: 上传xml文件")
    write_run_record(f"{datetime.datetime.now()}: 上传xml文件")

    # 将文件上抛
    ftp_server.storbinary('STOR %s' % remote_file_name, fp, buf_size)
    ftp_server.set_debuglevel(0)

    # 关键节点写入日志
    print(f"{datetime.datetime.now()}: 结束上传")
    write_run_record(f"{datetime.datetime.now()}: 结束上传")

    # 关闭本地文件
    fp.close()
    # 退出ftp服务器
    ftp_server.quit()

    return 0


class MyHandler(FileSystemEventHandler):
    """看门狗watchdog类，用于监控log生成"""
    def on_created(self, event):
        """监控文件是否被创建，如果被创建则触发相应自定义函数"""
        # 获取被创建的文件名称（含后缀名）&后缀名
        created_file_name = os.path.basename(event.src_path)
        created_file_type = os.path.splitext(created_file_name)[-1][1:].lower()  # 获取被清洗文件文件的后缀名

        # 判断是否为csv文件
        if created_file_type == "csv":
            # 关键节点写入日志
            print(f"{datetime.datetime.now()}: 文件被创建:【{event.src_path}】")
            write_run_record(f"{datetime.datetime.now()}: 文件被创建:【{event.src_path}】")

        # 判断是否为csv文件
        if created_file_type == "csv":
            # 检测xml文件夹是否存在
            xml_path = "./xml/"
            if not os.path.exists(xml_path):
                os.makedirs(xml_path)

            # 关键节点写入日志
            print(f"{datetime.datetime.now()}: 开始解析csv文件【{created_file_name}】")
            write_run_record(f"{datetime.datetime.now()}: 开始解析csv文件【{created_file_name}】")
            try:
                try:
                    # 将csv格式转换为xml格式
                    xml_file = csv_to_xml(event.src_path, xml_path)
                except:
                    xml_file = ""

                if len(str(xml_file)) > 1:
                    # 关键节点写入日志
                    print(f"{datetime.datetime.now()}: 成功解析csv文件")
                    write_run_record(f"{datetime.datetime.now()}: 成功解析csv文件")
                else:
                    # 关键节点写入日志
                    print(f"{datetime.datetime.now()}: 失败解析csv文件")
                    write_run_record(f"{datetime.datetime.now()}: 失败解析csv文件")

                # 关键节点写入日志
                print(f"{datetime.datetime.now()}: 开始上传xml文件【{xml_file}】")
                write_run_record(f"{datetime.datetime.now()}: 开始上上传xml文件【{xml_file}】")

                try:
                    # 将xml文件上抛至ftp服务器
                    upload_result = upload_xml_file(xml_file, created_file_name)
                except:
                    upload_result = 1

                if upload_result == 0:
                    # 关键节点写入日志
                    print(f"{datetime.datetime.now()}: 成功上传xml文件【{xml_file}】")
                    write_run_record(f"{datetime.datetime.now()}: 成功上传xml文件【{xml_file}】")
                else:
                    # 关键节点写入日志
                    print(f"{datetime.datetime.now()}: 失败上传xml文件【{xml_file}】")
                    write_run_record(f"{datetime.datetime.now()}: 失败上传xml文件【{xml_file}】")
            finally:
                # 关键节点写入日志
                print(f"{datetime.datetime.now()}: 文件处理完成：【{event.src_path}】\n")
                write_run_record(f"{datetime.datetime.now()}: 文件处理完成：【{event.src_path}】\n")


def start_watchdog(monitor_path):
    """启动看门狗程式"""
    # 创建看门狗watchdog实例并运行。
    path = monitor_path  # 被监控文件夹的路径,即ALS生成log的位置

    # 创建watchdog实例
    event_handler = MyHandler()

    # 关键节点写入日志
    print(f"{datetime.datetime.now()}: 看门狗程式启动中，监控路径：【{path}】")
    write_run_record(f"{datetime.datetime.now()}: 看门狗程式启动中，监控路径：【{path}】")

    # 开启服务
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()

    # 关键节点写入日志
    print(f"{datetime.datetime.now()}: 看门狗程式启动完成，持续监控中...\n")
    write_run_record(f"{datetime.datetime.now()}: 看门狗程式启动完成，持续监控中...\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()


if __name__ == '__main__':
    # 创建日志文件
    print(f"{datetime.datetime.now()}: ALS LOG档 上抛程式启动,准备创建日志文件")
    write_run_record(f"{datetime.datetime.now()}: ALS LOG档 上抛程式启动,准备创建日志文件")

    print(f"{datetime.datetime.now()}: 日志文件创建成功，日志路径：【./record/{str(datetime.datetime.now().strftime('%Y_%m_%d'))}.txt】\n")
    write_run_record(f"{datetime.datetime.now()}: 日志文件创建成功，日志路径：【./record/{str(datetime.datetime.now().strftime('%Y_%m_%d'))}.txt】\n")

    # 启动watchdog函数
    start_watchdog("D:/ALS/")
