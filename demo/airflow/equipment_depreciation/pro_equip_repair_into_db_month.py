# coding:utf-8
import pymysql
import json
from datetime import datetime
import time

conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=63306,
                       db='cloudteam', cursorclass=pymysql.cursors.DictCursor)  # 使用字典游标查询)


def getEquipmentTypeId(typeId):

    while(typeId != 'L0'):
        selectType = '''
            select type_name,type_sup_id from isyscore_equipment_type_info
            where id = '%s' 
        ''' %(typeId)
        cursor.execute(selectType)
        result  = cursor.fetchone()
        typeSupId = result['type_sup_id']
        typeId = typeSupId

    cursor.execute(selectType)
    equipmentType = cursor.fetchone()
    if(equipmentType['type_name'] == '设备'):
        return '1'
    else:
        return '2'


def getAttendDays():
    # 查询当前日历工作日天数
    selectDays = '''
                              select record.record_data
                              from isyscore_form_record record
                              left join isyscore_form_info info
                              on record.form_id = info.id
                              where record.del_flag = '0' and info.del_flag = '0'
                              and info.form_name = '生产日历' 
                                  and date_format(record.create_time,'%Y-%m') =  
                              date_format(now(),'%Y-%m');  #'%Y-%m-%d %H:%i:%s'
                      '''
    cursor.execute(selectDays)
    daysJson = cursor.fetchone()
    # 获取行的json
    days_data = json.loads(daysJson['record_data'])
    # 获取josn的record字段，得到一个数组
    recordDataArray = list(map(lambda x: x['record'], days_data))
    record = recordDataArray[1]
    totalDays = len(str(record).split(","))  # 当前月考勤的天数
    return totalDays



with conn.cursor() as cursor:

     now = datetime.now()
     nowDate = now.strftime("%Y-%m")
     deleteRecord = '''
        delete from cloudteam_data_warehouse.st_equip_repair_info 
        where ymd = '%s'
     '''  %(nowDate)
     cursor.execute(deleteRecord)

     selectRecord = '''
                   select record.record_data
                   from isyscore_form_record record
                   left join isyscore_form_info info
                   on record.form_id = info.id
                   where record.del_flag = '0' and info.del_flag = '0'
                   and info.form_name = '折旧表' 
           '''
     cursor.execute(selectRecord)

     for row in cursor.fetchall():
         recorddata = row['record_data']
         recorddata_json = json.loads(recorddata)
         recordArray = list(map(lambda x:x['record'], recorddata_json))
         equipmentId = recordArray[0]
         depreciatPrice =  recordArray[1]
         repairedDate =  recordArray[2]

         dailyTime = int(repairedDate)
         timeArray = time.localtime(dailyTime / 1000)
         ymd = time.strftime("%Y-%m", timeArray)

         if(ymd == nowDate):
             selectEquipment = '''
                select name,type_id from isyscore_equipement_info 
                where id = '%s'
             ''' %(equipmentId)

             cursor.execute(selectEquipment)
             equipment =  cursor.fetchone()
             typeId = equipment['type_id']
             typeCode = getEquipmentTypeId(typeId)
             perCost = depreciatPrice/getAttendDays()
             insertReqaired = '''
                insert into cloudteam_data_warehouse.st_equip_repair_info values (%s,%s,%s,%s,%s)
             '''
             cursor.execute(insertReqaired,[nowDate,equipment['name'],equipmentId,perCost,typeCode])
             conn.commit()










































