# coding:utf-8
import pymysql
import numpy as np
import pandas as pd
import json
from utils_p import string_datetime,return_n_s
from datetime import datetime
from functools import reduce
import os
import configparser
cf = configparser.ConfigParser()
cf.read("/home/airflow/airflow/dags/cloudteam_pre/update_time.cnf")
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
options = cf['tables']

table_of_form = {
    "生产日报": "ST_PRODUCTION_TASK",
    "生产日历": "ST_PRODUCTION_CALENDAR",
    "能源表": "ST_ENERGY",
    "5S记录表": "ST_5S_RECORD",
    "改善项目管理记录表":"ST_IMPROVMENT",
    "品质管理记录":"ST_QUANTITY_RECORD",
    "安全问题记录表":"ST_SAFETY_RECORD",
    "OJT记录表":"ST_OJT_RECORD",
    "物资领用记录表":"ST_SOURCE_RECORD",
    "设备故障记录表":"ST_DEVICE_BROKEN",
    "停线记录表":"ST_SCHEDULE_DELAY",
    "维修费用表":"ST_DEVICE_REPAIR",
    "折旧表":"ST_DEVICE_DEPRECIATION",
    "员工考勤表":"ST_CHECK_WORK",
    "设备运行记录表":"DW_DEVICE_OPERATION"
}



conn = pymysql.connect(host='10.20.3.7', user='root', password='Isysc0re', port=23306, db='cloudteam')
cursor = conn.cursor()

# 班组映射
workshop_df = pd.read_sql("select id,wt_name from isyscore_work_team_info;", conn, index_col=None)
workshop_df.columns = ['id', "wt_name"]
workshop_map = workshop_df.groupby('id')['wt_name'].apply(list).to_dict()

# field映射
field_df = pd.read_sql("select id,field_lable from isyscore_form_field;", conn, index_col=None)
field_df.columns = ['id', "field_lable"]
field_map = field_df.groupby('id')['field_lable'].apply(list).to_dict()

# 产品映射
product_df = pd.read_sql("select id,produce_name from isyscore_produce_info;", conn, index_col=None)
product_df.columns = ['id', "product_name"]
product_map = product_df.groupby('id')['product_name'].apply(list).to_dict()

# 勾选项映射
options_df = pd.read_sql("select field_id,diy_label,diy_value from isyscore_form_field_config;", conn, index_col=None)
options_df.columns = ['field_id', 'diy_label', "diy_value"]
options_map = options_df.groupby(['field_id', 'diy_value'])['diy_label'].apply(list).to_dict()

# 物料类型映射
source_df = pd.read_sql("select id,source_type from isyscore_source_info;", conn, index_col=None)
source_df.columns = ['id','source_type']
source_map = source_df.groupby(['id'])['source_type'].apply(list).to_dict()

# 物料价格映射
source_price_df = pd.read_sql("select id,source_price from isyscore_source_info", conn, index_col=None)
source_price_df.columns = ['source_id','price']
source_price_map = source_price_df.groupby(['source_id'])['price'].apply(list).to_dict()


def process_ymd(data):
    if "/" in str(data):
        ymd = datetime.strptime(data, "%Y/%m/%d").date()
    elif "-" in str(data):
        ymd = datetime.strptime(data, "%Y-%m-%d").date()
    else:
        if len(str(data))==10:
            ymd = datetime.fromtimestamp(data).date()
        else:
            ymd = datetime.fromtimestamp(data/1000).date()

    return ymd

def process_data(record_data, row, field_id_list):
    try:
        if (row['form_name'] == '生产日报'):
            print(record_data)
            ymd = process_ymd(record_data[1])
            workshop_name = workshop_map[row['wt_id']]
            product_name = product_map[record_data[0]]
            data = [ymd,row['wt_id'],workshop_name,record_data[0],product_name,record_data[3],record_data[4],record_data[5],record_data[6]]
            return [data]


        elif (row['form_name'] == '生产日历'):
            ym = process_ymd(record_data[0])
            year = ym.year
            month = ym.month
            data = []
            days = str(record_data[1]).split(",")
            options = list(map(lambda x: options_map[(field_id_list[1], x)][0], days))
            for i in options:
                try:
                    ym = datetime(year, month, int(i)).date()
                    tmp = [ym, 1, row['timeStamp']]
                    data.append(tmp)
                except ValueError:
                    continue
            return data
        elif (row['form_name'] == '能源表'):
            ym = process_ymd(record_data[-1])
            record_data[-1] = ym
            data = [record_data[1], record_data[2], record_data[4], record_data[5], record_data[7], record_data[8],
                    record_data[9]]
            return [data]
        elif (row['form_name'] == '5S记录表'):
            #record_data = list(map(lambda x:True if(x=='true') else False,record_data))
            is_5s = reduce(lambda x, y: x and y, record_data)
            workshop_id = row['wt_id']
            ymd = row['create_time'].date()
            return [[ymd,workshop_map[workshop_id], '完成' if is_5s is True else '未完成']]
        elif (row['form_name'] == '改善项目管理记录表'):
            if(record_data[1] is not None and record_data[1]!=''):
                ymd = process_ymd(record_data[1])
            else:
                ymd = row['create_time'].date()
            workshop_id = row['wt_id']
            if(record_data[0].isdigit()==False):
                record_data[0] = 0
            if(record_data[-1].isdigit()==False):
                record_data[-1] = 0
            data = [ymd,workshop_id,record_data[0],record_data[-1]]
            return [data]
        elif(row['form_name'] == '品质管理记录'):
            ymd = row['create_time'].date()
            workshop_id = row['wt_id']
            workshop_name = workshop_map[workshop_id]
            product_name = product_map[record_data[1]]
            if(record_data[7].isdigit()==False):
                record_data[7] = 0
            if(record_data[8].isdigit()==False):
                record_data[8] = 0
            if(record_data[11].isdigit()==False):
                record_data[11] = 0

            data = [ymd,record_data[1],product_name,workshop_id,workshop_name,options_map[(field_id_list[2],record_data[2])][0],
                    record_data[7],record_data[8],record_data[11],options_map[(field_id_list[-2],record_data[-2])]]
            print(data)
            return [data]
        elif(row['form_name'] == '安全问题记录表'):
            ymd = process_ymd(record_data[1])
            workshop_id = row['wt_id']
            data = [ymd,workshop_id,record_data[3],options_map[(field_id_list[4],record_data[4])][0],options_map[(field_id_list[-5],record_data[-5])][0],0 if record_data[8] == '' or record_data is None else record_data[8],0 if record_data[9]=='' or record_data[9] is None else record_data[9]]
            print(data)
            return [data]
        elif(row['form_name'] == 'OJT记录表'):
            ymd = row['create_time'].date()
            workshop_id = row['wt_id']
            if(record_data[5].isdigit()==False):
                record_data[5] = 0
            if(record_data[6].isdigit()==False):
                record_data[6] = 0
            data = [ymd,workshop_id,record_data[5],record_data[6]]
            print(data)
            return [data]
        elif(row['form_name'] == '物资领用记录表'):
            print(record_data)
            workshop_id = row['wt_id']
            ymd = datetime.fromtimestamp(record_data[-1]/1000).date()
            if(source_map[record_data[0]][0] =='辅料' or source_map[record_data[0]][0] == '工具'):
                type = '辅料'
            else:
                type = source_map[record_data[0]][0]
            if(record_data[1].isdigit()==False):
                record_data[1] = 0
            data = [ymd,workshop_id,record_data[0],type,source_price_map[record_data[0]],record_data[1]]
            print(data)
            return [data]
        elif(row['form_name']=='设备故障记录表'):
            ymd = row['create_time'].date()
            workshop_id = row['wt_id']
            data = [ymd,workshop_id,record_data[0],options_map[(field_id_list[1],record_data[1])],options_map[(field_id_list[4],record_data[4])],record_data[-1]]
            return [data]
        elif(row['form_name']=='停线记录表'):
            ymd =  row['create_time'].date()
            workshop_id = row['wt_id']
            data = [ymd,workshop_map[workshop_id],options_map[(field_id_list[0],record_data[0])],record_data[-1]]
            print(data)
            return [data]
        elif(row['form_name']=='维修费用表'):
            ymd = process_ymd(record_data[1])
            workshop_id = row['wt_id']
            data = [ymd,workshop_id,record_data[0],float(record_data[2])+float(record_data[3])+float(record_data[4])]
            print(data)
            return [data]
        elif(row['form_name']=='折旧表'):
            ymd = process_ymd(record_data[-1])
            year = ymd.year
            month = ymd.month
            new_ymd = datetime(year,month,1)
            workshop_id = row['wt_id']
            data = [ymd,workshop_id,record_data[0],options_map[(field_id_list[1],record_data[1])],record_data[2]]
            print(data)
            return [data]
        elif(row['form_name']=='员工考勤表'):
            ymd = process_ymd(record_data[1])
            workshop_id = row['wt_id']
            data = [ymd,workshop_id,record_data[0],options_map[(field_id_list[2],record_data[2])]]
            print(data)
            return [data]

        elif(row['form_name']=='设备运行记录表'):
            ymd = row['create_time'].date()
            workshop_id = row['wt_id']
            print("------0--", record_data[0], "-----------")
            print("------2--",record_data[2]/1000,"-----------")
            print("------1--", record_data[1]/1000, "-----------")
            work_time = datetime.fromtimestamp(record_data[2]/1000)-datetime.fromtimestamp(record_data[1]/1000)
            # work_time = datetime.strptime(str(ymd)+" "+record_data[2]+":00","%Y-%m-%d %H:%M:%S")-datetime.strptime(str(ymd)+" "+record_data[1]+":00","%Y-%m-%d %H:%M:%S")
            print("------4--", record_data[4]/1000, "-----------")
            print("------3--", record_data[3]/1000, "-----------")
            plan_stop_time = datetime.fromtimestamp(record_data[4]/1000)-datetime.fromtimestamp(record_data[3]/1000)
            # plan_stop_time = datetime.strptime(str(ymd)+" "+record_data[4]+":00","%Y-%m-%d %H:%M:%S")-datetime.strptime(str(ymd)+" "+record_data[3]+":00","%Y-%m-%d %H:%M:%S")
            data = [ymd,workshop_id,record_data[0],work_time.seconds/60,plan_stop_time.seconds/60]
            print(data)
            return [data]
    except KeyError as e:
        print(e)
        print(record_data)
        print("插入失败")
        return []
    return []

# if(options['isyscore_form_record'] == 'None'):
#     record_sql = '''
#     select form_name,wt_id,record_data,isyscore_form_record.create_time from isyscore_form_record
#     left join isyscore_form_info on isyscore_form_record.form_id = isyscore_form_info.id
#     where isyscore_form_info.del_flag = 0 and form_name not like '%test%'
#     and (isyscore_form_record.wt_id is not null or form_name like '%生产日历%')
#     order by create_time asc;
#     '''
# else:
record_sql = '''
    select form_name,wt_id,record_data,isyscore_form_record.create_time from isyscore_form_record
    left join isyscore_form_info on isyscore_form_record.form_id = isyscore_form_info.id
    where isyscore_form_info.del_flag = '0' and form_name not like '%%test%%'
    and (isyscore_form_record.wt_id is not null or form_name ='生产日历') and isyscore_form_record.create_time > ''
    and form_name = '折旧表' 
    order by create_time asc;
    '''  #% (options['isyscore_form_record'])

record_df = pd.read_sql(record_sql, conn)
record_df.columns = ['form_name', 'wt_id', 'record_data', 'create_time']
record_df['timeStamp'] = pd.to_datetime(record_df['create_time']).apply(string_datetime)
max_time = None
for index, row in record_df.iterrows():
    # 不需要用到的表单不处理
    if(row['form_name']) not in table_of_form.keys():
        continue
    json_data = json.loads(row['record_data'])
    record_data = list(map(lambda x: x['record'], json_data))

    record = list(map(lambda x: field_map[x['fieldId']][0], json_data))
    field_id_list = list(map(lambda x: x['fieldId'], json_data))
    # 列名1
    print(record)
    # 表单名
    print(row['form_name'])
    # 源数据
    print(record_data)  #record 的值
    print(field_id_list)   #field 的id
    # # 处理json数据成事实表格式
    record_data = process_data(record_data, row, field_id_list)
    if(record_data == []):
        continue
    # 根据表单类别决定插入的数据表
    insert_table = table_of_form[row['form_name']]
    insert_sql = "insert into cloudteam_data_warehouse." + insert_table + " values(" + return_n_s(len(record_data[0])) + ")"
    # insert_sql = "insert into " + insert_table + " values(" + return_n_s(len(record_data[0])) + ")"
    try:
        cursor.executemany(insert_sql, record_data)
        conn.commit()
    except pymysql.err.IntegrityError:
        pass
    max_time = row['create_time']

    #    *    *    *     *     *    *      *
    #    秒   分    时    日    月   星期    年
    #    13   13   15    20    *    ?      (可选，留空)                    通用的标识符  , - * /
    #    cron表达式 ： 6或7个域
    #    *   /10    *     *    *    *

    #    airflow cron 表达式: * * * * * * (分 时 月 年 周 秒)


# if(max_time is not None):
#     cf.set("tables","isyscore_form_record",max_time.strftime("%Y-%m-%d %H:%M:%S"))
#     with open("/home/airflow/airflow/dags/cloudteam_pre/update_time_new.cnf","w+") as f:
#         cf.write(f)






