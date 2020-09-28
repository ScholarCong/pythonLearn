# coding:utf-8
import pymysql
import json
import configparser
import time

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

start_time = '2020-07-01'
end_time = '2020-08-01'

with conn.cursor() as cursor:

    selectEnergyInfo = '''
          select record.record_data
                          from isyscore_form_record record
                          left join isyscore_form_info info
                          on record.form_id = info.id
                          where record.del_flag = '0' and info.del_flag = '0'
                          and info.form_name = '能源表' 
                    limit 0,1
    '''
    cursor.execute(selectEnergyInfo)
    resultOne = cursor.fetchone()
    result = resultOne['record_data']
    jsonData = json.loads(result)
    recordDataArray = list(map(lambda x: x['fieldId'], jsonData))
    timeFieldId = str(recordDataArray[0])

    if (start_time == None and start_time == None):
        selectRecord = '''
            select record_data from isyscore_form_record record
            where id in 
            (
            select search_id from isyscore_form_seach_index search
            where field_id = '%s' and field_data = %s
            and search.del_flag = '0'
            )
            and record.del_flag = '0'
        ''' %(timeFieldId,time.time())
    else:
        #删除这个时间段统计的数据
        deleteData = '''
            delete from cloudteam_data_warehouse.st_energy_daily_cost
            where ymd >= '%s' and ymd <= '%s'
        ''' %(start_time,end_time)
        cursor.execute(deleteData)

        start = int(time.mktime(time.strptime(start_time, "%Y-%m-%d"))) * 1000
        end = int(time.mktime(time.strptime(end_time, "%Y-%m-%d"))) * 1000

        selectRecord = '''
                   select record_data from isyscore_form_record record
                   where id in 
                   (
                       select search_id from isyscore_form_seach_index search
                       where field_id = '%s' and field_data >= %s
                       and field_data <= %s
                       and search.del_flag = '0'
                   )
                   and record.del_flag = '0'
               ''' % (timeFieldId,start,end)

    cursor.execute(selectRecord)
    for row in cursor.fetchall():
        recordData = row['record_data']
        json_data = json.loads(recordData)
        recordArray = list(map(lambda x: x['record'], json_data))

        ymd = time.strftime("%Y-%m-%d", time.localtime(recordArray[0]/1000))
        insertDaily = '''
            insert into cloudteam_data_warehouse.st_energy_daily_cost values
            ('%s',%s,%s,%s,%s,%s,%s)
        ''' %(ymd,recordArray[2],recordArray[3],recordArray[5],recordArray[6],recordArray[8],recordArray[9])
        cursor.execute(insertDaily)

        conn.commit()


















































