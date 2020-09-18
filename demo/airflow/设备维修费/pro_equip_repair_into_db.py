# coding:utf-8
import pymysql
import json
from datetime import datetime
import decimal

conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=63306,
                       db='cloudteam', cursorclass=pymysql.cursors.DictCursor)  # 使用字典游标查询)

with conn.cursor() as cursor:

     selectRecord = '''
                select record.record_data
                from isyscore_form_record record
                left join isyscore_form_info info
                on record.form_id = info.id
                where record.del_flag = '0' and info.del_flag = '0'
                and info.form_name = '折旧表' 
                and date_format(record.create_time,'%Y-%m') =  
                date_format(now(),'%Y-%m');                      #'%Y-%m-%d %H:%i:%s'
        '''
     cursor.execute(selectRecord)

     now = datetime.now()
     nowDate = now.strftime("%Y-%m")

     for row in cursor.fetchall():
         recorddata = row['record_data']
         recorddata_json = json.loads(recorddata)
         recordArray = list(map(lambda x:x['record'], recorddata_json))
         equipmentId = recordArray[0]
         depreciatPrice =  recordArray[1]







































