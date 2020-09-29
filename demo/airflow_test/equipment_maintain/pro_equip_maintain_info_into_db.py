# coding:utf-8
import pymysql
import json
from datetime import datetime
import time
import configparser

conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=63306,
                       db='cloudteam', cursorclass=pymysql.cursors.DictCursor)  # 使用字典游标查询)

# cf = configparser.ConfigParser()
# cf.read("/home/airflow/airflow/dags/cloudteam_dev/start_time.cnf")
# cf.read("/home/airflow/airflow/dags/cloudteam_dev/end_time.cnf")
# options_start = cf['start_time']
# options_end = cf['end_time']
#
# start_time = options_start['start_time']
# end_time = options_end['end_time']

start_time = '2020-06-01'
end_time = '2020-08-01'

def return_n_s(columns):
    return "%s,"*(columns-1)+"%s"

def getMaintainTimeField():
    selectMaintainInfo = '''
               select record.record_data
                               from isyscore_form_record record
                               left join isyscore_form_info info
                               on record.form_id = info.id
                               where record.del_flag = '0' and info.del_flag = '0'
                               and info.form_name = '维修费用表' 
                         limit 0,1
         '''
    cursor.execute(selectMaintainInfo)
    resultOne = cursor.fetchone()
    result = resultOne['record_data']
    jsonData = json.loads(result)
    recordDataArray = list(map(lambda x: x['fieldId'], jsonData))
    timeFieldId = str(recordDataArray[1])
    return timeFieldId

with conn.cursor() as cursor:

    if (start_time == None and end_time == None):
        selectMaintainSql = '''
                select record_data from isyscore_form_record record
                left join isyscore_work_team_info info 
                on record.wt_id = info.id 
                where record.id in 
                (
                select search_id from isyscore_form_seach_index search
                where field_id = '%s' and field_data = %s
                and search.del_flag = '0'
                )
                and record.del_flag = '0'
        ''' %(getMaintainTimeField(),int(time.time()*1000))
    else:
        start = int(time.mktime(time.strptime(start_time, "%Y-%m-%d"))) * 1000
        end = int(time.mktime(time.strptime(end_time, "%Y-%m-%d"))) * 1000
        selectMaintainSql = '''
                       select record_data from isyscore_form_record record
                       left join isyscore_work_team_info info 
                       on record.wt_id = info.id 
                       where record.id in 
                       (
                       select search_id from isyscore_form_seach_index search
                       where field_id = '%s' and field_data >= %s
                       and field_data <= %s
                       and search.del_flag = '0'
                       )
                       and record.del_flag = '0'
               ''' % (getMaintainTimeField(),start,end)

    cursor.execute(selectMaintainSql)
    for row in cursor.fetchall():
        # subArray = []
        jsonData = json.loads(row['record_data'])
        recordDataArray = list(map(lambda x: x['record'], jsonData))
        timeArray = time.localtime(int(recordDataArray[1])/1000)
        ymd = time.strftime("%Y-%m-%d", timeArray)
        # for index in range(len(recordDataArray)):
        #     subArray.append(recordDataArray[index])
        insertSql = "insert into cloudteam_data_warehouse.st_equip_maintain_info values ("+ return_n_s(5) +")"

        cursor.execute(insertSql,[ymd,recordDataArray[0],recordDataArray[2],recordDataArray[3],recordDataArray[4]])
        conn.commit()

















































