# coding:utf-8
import pymysql
import numpy as np
import pandas as pd
import json
from utils_p import string_datetime, return_n_s
from datetime import datetime
from functools import reduce



# key - value 的map对象 #
table_of_form = {
    "生产日报": "ST_PRODUCTION_TASK",
    "生产日历": "ST_PRODUCTION_CALENDAR",
    "能源表": "ST_ENERGY",
    "5S记录表": "ST_5S_RECORD",
    "改善项目管理记录表": "ST_IMPROVMENT",
    "品质管理记录": "ST_QUANTITY_RECORD",
    "安全问题记录表": "ST_SAFETY_RECORD",
    "OJT记录表": "ST_OJT_RECORD",
    "物资领用记录表": "ST_SOURCE_RECORD",
    "设备故障记录表": "ST_DEVICE_BROKEN",
    "停线记录表": "ST_SCHEDULE_DELAY",
    "维修费用表": "ST_DEVICE_REPAIR",
    "折旧表": "ST_DEVICE_DEPRECIATION",
    "员工考勤表": "ST_CHECK_WORK",
    "设备运行记录表": "DW_DEVICE_OPERATION"
}

result = table_of_form.keys()
values = table_of_form.values()
value = table_of_form.get('OJT记录表')
print(result)
print(value)

if ('5S记录表' in result):
    print('true')


# 对 json 数据的解析 #

josnData = '[{"record": "切尔奇翁11122", "fieldId": "847b0599b1d511ea9629eec59bef4bba"}, ' \
       '{"record": "驱蚊器无", "fieldId": "847bd9fcb1d511ea9629eec59bef4bba"}, ' \
       '{"record": "1", "fieldId": "847c9569b1d511ea9629eec59bef4bba"}, ' \
       '{"record": "9b0fabd250452fd0b270fab6bb26a5e6", "fieldId": "847ee57eb1d511ea9629eec59bef4bba"},' \
       ' {"record": "", "fieldId": "847f9f49b1d511ea9629eec59bef4bba"}, ' \
       '{"record": "", "fieldId": "84828b77b1d511ea9629eec59bef4bba"}, ' \
       '{"record": "2", "fieldId": "8483ee71b1d511ea9629eec59bef4bba"}, ' \
       '{"record": "1a85f428cdc103f33de57193d1d617d6", "fieldId": "8486291db1d511ea9629eec59bef4bba"}, ' \
       '{"record": "2020-06-26", "fieldId": "8486d01cb1d511ea9629eec59bef4bba"}, ' \
       '{"record": "1", "fieldId": "848772f1b1d511ea9629eec59bef4bba"}]'

jsonResult = json.loads(josnData)
print(jsonResult)
record_data = list(map(lambda x: x['record'], jsonResult))
print(record_data)
print(record_data[0])


#  采用 pandas 操作数据库 #

conn = pymysql.connect(host='10.20.3.7', user='root', password='Isysc0re', port=23306, db='cloudteam')
cursor = conn.cursor()

workshop_df = pd.read_sql("select id,wt_name from isyscore_work_team_info;", conn, index_col=None)
workshop_df.columns = ['id', "wt_name"]
workshop_map = workshop_df.groupby('id')['wt_name'].apply(list).to_dict()  # 返回了一个map对象
print(workshop_map)
keys = workshop_map.keys()
print(keys)