# coding:utf-8
import pymysql
import json
from datetime import datetime
import time
import configparser

conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=63306,
                       db='cloudteam', cursorclass=pymysql.cursors.DictCursor)  # 使用字典游标查询)

cf = configparser.ConfigParser()
cf.read("/home/airflow/airflow/dags/cloudteam_dev/start_time.cnf")
cf.read("/home/airflow/airflow/dags/cloudteam_dev/end_time.cnf")
options_start = cf['start_time']
options_end = cf['end_time']

start_time = options_start['start_time']
end_time = options_end['end_time']

# start_time = '2020-06'
# end_time = '2020-08'

#查询当前日期区间的算法
def getDates(start_time,end_time):
    startArrayDate = start_time.split('-')
    endArrayDate = end_time.split('-')
    years = int(endArrayDate[0]) - int(startArrayDate[0])
    months = int(endArrayDate[1]) - int(startArrayDate[1])
    monthes = years * 12 + months
    index = 0
    arrayDate = []
    year = int(startArrayDate[0])
    date = int(startArrayDate[1])

    while (index <= monthes):
        if (date < 10):
            strDate = str(year) + '-0' + str(date)
        else:
            strDate = str(year) + '-' + str(date)

        arrayDate.append(strDate)
        date += 1
        if (date > 12):
            date = 1
            year += 1
        index += 1

    return arrayDate





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


def getTimeFieldId():
    selectDialyInfo = '''
               select record.record_data
                               from isyscore_form_record record
                               left join isyscore_form_info info
                               on record.form_id = info.id
                               where record.del_flag = '0' and info.del_flag = '0'
                               and info.form_name = '生产日历' 
                         limit 0,1
         '''
    cursor.execute(selectDialyInfo)
    resultOne = cursor.fetchone()
    result = resultOne['record_data']
    jsonData = json.loads(result)
    recordDataArray = list(map(lambda x: x['fieldId'], jsonData))
    timeFieldId = str(recordDataArray[0])
    return timeFieldId

def getAttendDays(ymdd):
    # 查询当前日历工作日天数
    selectDays = '''
                              select record.record_data
                              from isyscore_form_record record
                              left join isyscore_form_info info
                              on record.form_id = info.id
                              where record.del_flag = '0' and info.del_flag = '0'
                              and info.form_name = '生产日历' 
                      '''
    cursor.execute(selectDays)
    daysJson = cursor.fetchall()
    for row in daysJson:
        # 获取行的json
        days_data = json.loads(row['record_data'])
        # 获取josn的record字段，得到一个数组
        recordDataArray = list(map(lambda x: x['record'], days_data))
        dateNum = int(recordDataArray[0])
        timeArray = time.localtime(dateNum / 1000)
        ymd = time.strftime("%Y-%m", timeArray)
        if(ymd == ymdd):
            totalDays = len(str(recordDataArray[1]).split(","))  # 当前月考勤的天数
            return totalDays


def insertEquipRepair(nowDate):
    deleteRecord = '''
           delete from cloudteam_data_warehouse.st_equip_repair_info 
           where ymd = '%s'
        ''' % (nowDate)
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
        recordArray = list(map(lambda x: x['record'], recorddata_json))
        equipmentId = recordArray[0]
        depreciatPrice = recordArray[1]
        repairedDate = recordArray[2]

        dailyTime = int(repairedDate)
        timeArray = time.localtime(dailyTime / 1000)
        ymd = time.strftime("%Y-%m", timeArray)

        if (ymd == nowDate):
            selectEquipment = '''
                   select name,type_id from isyscore_equipement_info 
                   where id = '%s'
                ''' % (equipmentId)

            cursor.execute(selectEquipment)
            equipment = cursor.fetchone()
            typeId = equipment['type_id']
            typeCode = getEquipmentTypeId(typeId)
            perCost = int(depreciatPrice) / getAttendDays(ymd)
            insertReqaired = '''
                   insert into cloudteam_data_warehouse.st_equip_repair_info values (%s,%s,%s,%s,%s)
                '''
            cursor.execute(insertReqaired, [nowDate, equipment['name'], equipmentId, perCost, typeCode])
            conn.commit()

with conn.cursor() as cursor:

     if(start_time == None and end_time == None):
         now = datetime.now()
         nowDate = now.strftime("%Y-%m")
         insertEquipRepair(nowDate)
     else:
        dateArray = getDates(start_time,end_time)
        for index in range(len(dateArray)):
            strDate = dateArray[index]
            insertEquipRepair(strDate)











































