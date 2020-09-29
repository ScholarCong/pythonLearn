# coding:utf-8
import pymysql
import json
from datetime import datetime
import configparser

conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=63306,
                       db='cloudteam', cursorclass=pymysql.cursors.DictCursor)  # 使用字典游标查询)


# cf = configparser.ConfigParser()
# cf.read("/home/airflow/airflow/dags/cloudteam_dev/start_time.cnf")
# cf.read("/home/airflow/airflow/dags/cloudteam_dev/end_time.cnf")
# options_start = cf['start_time']
# options_end = cf['end_time']

# start_time = options_start['start_time']
# end_time = options_end['end_time']

start_time = '2020-07-01'
end_time = '2020-08-01'

def getPros():

    if (start_time == None and end_time == None):
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

def getYmdEquips(ymd):
    # 根据产品查出当天产品所使用的设备列表
    selectYmdEquips = '''
        select ymd,equipment_id, maintain_cost+ready_cost+other_cost  totalCost
		from cloudteam_data_warehouse.st_equip_maintain_info info
		where ymd = '%s'
    ''' %(ymd)
    cursor.execute(selectYmdEquips)
    array = []
    for row2 in cursor.fetchall():
        dict = {}
        dict['equipId'] = row2['equipment_id']
        dict['totalCost'] = row2['totalCost']
        array.append(dict)
    return array


with conn.cursor() as cursor:
    if (start_time == None and start_time == None):
        insertSql = '''
                       insert into cloudteam_data_warehouse.dw_production_cost 
                       (ymd,product_name,product_id,dimension,sum)
                       select 
                           record.ymd,
                           record.product_name,
                           record.product_id,
                           '设备维修费',
                           (
                                select distinct actural_pass_num 
                                from cloudteam_data_warehouse.st_production_daily_record 
                                where product_id = record.product_id
                                and is_last = 1 and ymd = date_format(now(),'%Y-%m-%d')
                           ) sum 
                       from cloudteam_data_warehouse.st_production_daily_record record
                       where record.ymd = date_format(now(),'%Y-%m-%d')
                       and record.wt_type = '1'
                       group by record.product_id '''
    else:
        # 删除当前天的计算数据
        deleteSql = '''
                               delete from cloudteam_data_warehouse.dw_production_cost
                               where ymd >= %s and ymd <= %s 
                               and dimension = '设备维修费'
                           '''
        cursor.execute(deleteSql, [start_time, end_time])
        insertSql = '''
                             insert into cloudteam_data_warehouse.dw_production_cost 
                             (ymd,product_name,product_id,dimension,sum)
                             select 
                                 record.ymd,
                                 record.product_name,
                                 record.product_id,
                                 '设备维修费',
                                 (
                                      select distinct actural_pass_num 
                                      from cloudteam_data_warehouse.st_production_daily_record 
                                      where product_id = record.product_id
                                      and is_last = 1 and ymd = record.ymd
                                 ) sum 
                             from cloudteam_data_warehouse.st_production_daily_record record
                             where record.ymd >= '%s'
                             and record.ymd <= '%s'
                             and record.wt_type = '1'
                             group by record.ymd,record.product_id 
                         ''' % (start_time, end_time)

    cursor.execute(insertSql)

    # 计算产品的设备折旧费
    if (start_time == None and start_time == None):
        selectPro = '''
                     select product_id,sum,ymd from cloudteam_data_warehouse.dw_production_cost 
                     where ymd = date_format(now(),'%Y-%m-%d') and dimension = '设备维修费'
                 '''
    else:
        selectPro = '''
                            select product_id,sum,ymd from cloudteam_data_warehouse.dw_production_cost 
                            where ymd >= '%s' 
                            and ymd <= '%s'
                            and dimension = '设备维修费'
                        ''' % (start_time, end_time)

    cursor.execute(selectPro)
    for row in cursor.fetchall():
        productId = row['product_id']
        ymd = row['ymd']
        sum = row['sum']
        # 查询当前产品所使用的设备
        selectEquips = '''
                   select equipRel.equipment_id from isyscore_product_sequence_rel sequenceRel 
                   left join isyscore_pro_seq_equip_rel equipRel 
                   on sequenceRel.id = equipRel.seq_pro_rel_id
                   where sequenceRel.del_flag = '0' and equipRel.del_flag = '0'
                   and sequenceRel.product_id = '%s'
                   and equipRel.is_select = '1'
               ''' % (productId)
        cursor.execute(selectEquips)
        equipIdArray = []
        totalCost = 0
        for row1 in cursor.fetchall():
            equipId = row1['equipment_id']
            equipArray = getYmdEquips(ymd)
            for index in range(len(equipArray)):
                if(equipId == equipArray[index]['equipId']):
                    partcost = equipArray[index]['totalCost']
                    # 计算分摊规则3
                    selectEquipPros = '''
                                   select sequenceRel.product_id from isyscore_pro_seq_equip_rel equipRel
                                   left join isyscore_product_sequence_rel sequenceRel 
                                   on sequenceRel.id = equipRel.seq_pro_rel_id
                                   where sequenceRel.del_flag = '0' and equipRel.del_flag = '0'
                                   and  equipRel.equipment_id = '%s'
                                   group by sequenceRel.product_id
                               ''' % (equipId)
                    cursor.execute(selectEquipPros)
                    equipSelectProIdArray = []  # 该设备今天生产的所有的产品的id
                    for row1 in cursor.fetchall():
                        prodId = row1['product_id']
                        if (prodId in getPros()):
                            equipSelectProIdArray.append(prodId)
                        else:
                            continue

                    # 计算分摊规则3
                    totalTimeCost = 0
                    currentCost = 0
                    for index in range(len(equipSelectProIdArray)):
                        proId = equipSelectProIdArray[index]
                        selectTime = '''
                                    select distinct sequenceRel.sequence_standare_time time from isyscore_pro_seq_equip_rel equipRel
                                    left join isyscore_product_sequence_rel sequenceRel 
                                    on sequenceRel.id = equipRel.seq_pro_rel_id
                                    where sequenceRel.del_flag = '0' and equipRel.del_flag = '0'
                                    and  equipRel.equipment_id = '%s'
                                    and sequenceRel.product_id = '%s'
                                ''' % (equipId, proId)
                        cursor.execute(selectTime)
                        stardardTime = cursor.fetchone()  # 当前工序的标准工时
                        selectPassNum = '''
                                     select actural_pass_num from cloudteam_data_warehouse.st_production_daily_record record
                                     where record.ymd = '%s' and record.product_id = '%s'
                                     and record.is_last = 1
                                ''' % (ymd, proId)
                        cursor.execute(selectPassNum)
                        passNum = cursor.fetchone()  # 当前产品的合格数量
                        if (stardardTime == None and passNum == None and stardardTime['time'] == None and passNum[
                            'actural_pass_num'] == None):
                            continue

                        caculateTime = stardardTime['time'] * int(passNum['actural_pass_num'])
                        totalTimeCost += caculateTime
                        if (productId == proId):
                            currentCost = caculateTime

                    ratio = currentCost / totalTimeCost  # 分摊规则3比例
                    totalCost += partcost*float(ratio)
                    break
                else:
                    continue


        insertCost = '''
                       update cloudteam_data_warehouse.dw_production_cost set cost= %s
                       where ymd = '%s' and product_id = '%s' and dimension = '设备维修费'
                   ''' % (totalCost / sum, ymd, productId)
        cursor.execute(insertCost)

        conn.commit()















