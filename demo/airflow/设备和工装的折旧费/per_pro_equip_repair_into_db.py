# coding:utf-8
import pymysql
import json
from datetime import datetime
import time

conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=63306,
                       db='cloudteam', cursorclass=pymysql.cursors.DictCursor)  # 使用字典游标查询)

# cf = configparser.ConfigParser()
# cf.read("/home/airflow/airflow/dags/cloudteam_dev/start_time.cnf")
# cf.read("/home/airflow/airflow/dags/cloudteam_dev/end_time.cnf")
# options_start = cf['start_time']
# options_end = cf['end_time']

start_time = '2020-09-01'
end_time = '2020-10-01'

def getPros():
    # if (options_start['start_time'] == None and options_end['end_time'] == None):
    if (start_time == None and start_time == None):
        selectPros = '''
               select product_id from cloudteam_data_warehouse.st_production_daily_record record
               where record.ymd = date_format(now(),'%Y-%m-%d') 
               group by product_id
           '''
    else:
        selectPros = '''
                       select product_id from cloudteam_data_warehouse.st_production_daily_record record
                       where record.ymd >= '%s'
                       and record.ymd <= '%s'
                       group by product_id
                   ''' % (start_time, end_time)
    resultArray = []
    cursor.execute(selectPros)
    for row3 in cursor.fetchall():
        resultArray.append(row3['product_id'])

    return resultArray


with conn.cursor() as cursor:

    # if (options_start['start_time'] == None and options_end['end_time'] == None):
    if (start_time == None and start_time == None):
        selectPros = '''
            select product_id,ymd from cloudteam_data_warehouse.st_production_daily_record record
            where record.ymd = date_format(now(),'%Y-%m-%d') 
            group by record.ymd,product_id
        '''
    else:
        selectPros = '''
                    select product_id,ymd from cloudteam_data_warehouse.st_production_daily_record record
                    where record.ymd >= '%s'
                    and record.ymd <= '%s'
                    group by record.ymd,product_id
                ''' %(start_time,end_time)

    cursor.execute(selectPros)

    for row in cursor.fetchall():
        productId = row['product_id']
        ymd = row['ymd']
        #根据产品查出产品所使用的设备列表
        selectEquips = '''
            select equipRel.equipment_id from isyscore_product_sequence_rel sequenceRel 
            left join isyscore_pro_seq_equip_rel equipRel 
            on sequenceRel.id = equipRel.seq_pro_rel_id
            where sequenceRel.del_flag = '0' and equipRel.del_flag = '0'
            and sequenceRel.product_id = '%s'
            and equipRel.is_select = '1'
        ''' %(productId)
        cursor.execute(selectEquips)
        for row in cursor.fetchall():
            equipmentId = row['equipment_id']
            selectEquipPros = '''
                select sequenceRel.product_id from isyscore_pro_seq_equip_rel equipRel
                left join isyscore_product_sequence_rel sequenceRel 
                on sequenceRel.id = equipRel.seq_pro_rel_id
                where sequenceRel.del_flag = '0' and equipRel.del_flag = '0'
                and  equipRel.equipment_id = '%s'
                group by sequenceRel.product_id
            ''' %(equipmentId)
            cursor.execute(selectEquipPros)
            equipSelectProIdArray = []
            for row1 in cursor.fetchall():
                prodId = row1['product_id']
                if(prodId in getPros()):
                    equipSelectProIdArray.append(prodId)
                else:
                    continue
            totalCost = 0
            currentCost = 0
            for index in range(len(equipSelectProIdArray)):
                proId = equipSelectProIdArray[0]
                #sum * equipment time
                selectTime = '''
                    select distinct sequenceRel.sequence_standare_time time from isyscore_pro_seq_equip_rel equipRel
                    left join isyscore_product_sequence_rel sequenceRel 
                    on sequenceRel.id = equipRel.seq_pro_rel_id
                    where sequenceRel.del_flag = '0' and equipRel.del_flag = '0'
                    and  equipRel.equipment_id = '%s'
                    and sequenceRel.product_id = '%s'
                ''' %(equipmentId,proId)
                cursor.execute(selectTime)
                stardardTime = cursor.fetchone() #当前工序的标准工时
                selectPassNum = '''
                     select actural_pass_num from cloudteam_data_warehouse.st_production_daily_record record
                     where record.ymd = '%s' and record.product_id = '%s'
                     and record.is_last = 1
                ''' %(ymd,proId)
                cursor.execute(selectPassNum)
                passNum = cursor.fetchone() #当前产品的合格数量
                caculateTime = stardardTime['time']*int(passNum['actural_pass_num'])
                totalCost+=caculateTime
                if(productId == proId):
                    currentCost = caculateTime

            ratio = currentCost/totalCost  #分摊比例
            selectEquipCost = '''
                select cost from cloudteam_data_warehouse.st_equip_repair_info
                where ymd = '%s' and equip_id = '%s'
            ''' %(ymd[0:7],equipmentId)
            cursor.execute(selectEquipCost)
            costObj = cursor.fetchone()
            cost = costObj['cost']  #设备的维修平均一天的总费用
            proEquipCost = cost*float(ratio)  #当前设备在这天分摊到该产品总的维修费

            selectEquipType = '''
                select type from cloudteam_data_warehouse.st_equip_repair_info
                where equip_id = '%s'
                group by equip_id
            '''  %(equipmentId)
            cursor.execute(selectEquipType)
            type = cursor.fetchone()
            typeCode = type['type']

            insertSql = '''
                insert into cloudteam_data_warehouse.st_equip_pro_repair_cost values
                (%s,%s,%s,%s,%s)
            '''

            cursor.execute(insertSql,[ymd,equipmentId,productId,proEquipCost,typeCode])

        conn.commit()


























