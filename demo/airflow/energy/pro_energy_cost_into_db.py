# coding:utf-8
import pymysql
from datetime import datetime
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

# start_time = '2020-09-01'
# end_time = '2020-10-01'

def getRatio(ymd,productId):
    # 计算分摊规则2
    selectPros = '''
               select 
                   record.product_id,
                   (
                        select distinct actural_pass_num 
                        from cloudteam_data_warehouse.st_production_daily_record 
                        where product_id = record.product_id
                        and is_last = 1 and ymd = record.ymd
                   ) sum 
               from cloudteam_data_warehouse.st_production_daily_record record
               where record.ymd = '%s'
               and record.wt_type = '1'
               group by record.product_id 
       ''' % (ymd)
    cursor.execute(selectPros)
    sumTime = 0
    currenctProInfo = []
    for row4 in cursor.fetchall():
        productIdcur = row4['product_id']
        sum = row4['sum']
        selectTotalTime = '''
               select standard_time from cloudteam_data_warehouse.st_product_info
               where product_id = %s
           '''
        cursor.execute(selectTotalTime, productIdcur)
        standardTime = cursor.fetchone()
        totalTime = standardTime['standard_time']
        if (productId == productIdcur):
            currenctProInfo.append(totalTime)
            currenctProInfo.append(sum)

        if (totalTime != None):
            sumTime += (sum * totalTime)
    # 平摊比例
    currentProRatio = (currenctProInfo[0] * currenctProInfo[1]) / sumTime
    return currentProRatio





with conn.cursor() as cursor:

    # if (options_start['start_time'] == None and options_end['end_time'] == None):
    if (start_time == None and start_time == None):
        insertSql = '''
                insert into cloudteam_data_warehouse.dw_production_cost 
                (ymd,product_name,product_id,dimension,sum)
                select 
                    record.ymd,
                    record.product_name,
                    record.product_id,
                    '能源费',
                    (
                         select distinct actural_pass_num 
                         from cloudteam_data_warehouse.st_production_daily_record 
                         where product_id = record.product_id
                         and is_last = 1 and ymd = date_format(now(),'%Y-%m-%d')
                    ) sum 
                from cloudteam_data_warehouse.st_production_daily_record record
                where record.ymd = date_format(now(),'%Y-%m-%d')
                and record.wt_type = '1'
                group by record.product_id 
            '''
    else:
        # 删除当前天的计算数据
        deleteSql = '''
                         delete from cloudteam_data_warehouse.dw_production_cost
                         where ymd >= %s and ymd <= %s 
                         and dimension = '能源费'
                     '''
        # cursor.execute(deleteSql, [options_start['start_time'], options_end['end_time']])
        cursor.execute(deleteSql, [start_time, end_time])
        insertSql = '''
                       insert into cloudteam_data_warehouse.dw_production_cost 
                       (ymd,product_name,product_id,dimension,sum)
                       select 
                           record.ymd,
                           record.product_name,
                           record.product_id,
                           '能源费',
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
                   '''  %(start_time,end_time)

    cursor.execute(insertSql)

    #计算产品的能源费
    # if (options_start['start_time'] == None and options_end['end_time'] == None):
    if (start_time == None and start_time == None):
        selectPro = '''
                   select product_id,sum from cloudteam_data_warehouse.dw_production_cost 
                   where ymd = date_format(now(),'%Y-%m-%d') and dimension = '能源费'
               '''
    else:
        selectPro = '''
                          select product_id,sum,ymd from cloudteam_data_warehouse.dw_production_cost 
                          where ymd >= '%s' 
                          and ymd <= '%s'
                          and dimension = '能源费'
                      ''' % (start_time, end_time)

    cursor.execute(selectPro)
    for row in cursor.fetchall():
        proSum = row['sum']
        productId = row['product_id']
        now = datetime.now()
        ymd = now.strftime("%Y-%m-%d")

        if (start_time != None and end_time != None):
            ymd = row['ymd']

        selectEnergy = '''
            select ymd,water,water_price,electric,electric_price,gas,gas_price 
            from cloudteam_data_warehouse.st_energy_daily_cost
            where ymd = %s
        '''
        cursor.execute(selectEnergy,[ymd])
        result = cursor.fetchone()
        if(result != None):
            ymdWater = result['water']
            ymdGasprice = result['gas']
            ymdElectric = result['electric']
            #截至当前位置最近的一天
            selectMaxYmd = '''
                       select max(record.ymd) maxYmd
                        from (select ymd
                              from cloudteam_data_warehouse.st_energy_daily_cost
                              where ymd < '%s' 
                              order by ymd desc) record
                    ''' % (ymd)
            cursor.execute(selectMaxYmd)
            maxYmds = cursor.fetchone()
            cursor.execute(selectEnergy,[maxYmds['maxYmd']])
            energyInfo = cursor.fetchone()
            ymdWaterO = energyInfo['water']
            ymdGaspriceO = energyInfo['gas']
            ymdElectricO = energyInfo['electric']

            waterCost = (ymdWater-ymdWaterO)*result['water_price']  #水
            gasCost = (ymdGasprice-ymdGaspriceO)*result['gas_price']  #气
            electCost = (ymdElectric-ymdElectricO)*result['electric_price']  #电

            #设备用电
            selectEquipments = '''
                select seqRel.sequence_standare_time time ,info.power_consumpt
                from isyscore_pro_seq_equip_rel equipRel
                left join isyscore_product_sequence_rel seqRel 
                on equipRel.seq_pro_rel_id = seqRel.id
                left join isyscore_equipement_info info
                on info.id = equipRel.equipment_id
                where equipRel.del_flag = '0' and seqRel.del_flag = '0'
                and info.del_flag = '0' and equipRel.is_select = '1'
                and seqRel.product_id = '%s' 
            '''  %(productId)

            cursor.execute(selectEquipments)
            equipTotalCost = 0
            for row3 in cursor.fetchall():
                standarTime = row3['time']/3600 #换算时间为小时
                powerConsumpt = row3['power_consumpt']
                equipTotalCost += (int(standarTime)*float(powerConsumpt))
            if(equipTotalCost != 0):
                equipTotalCost = equipTotalCost*result['electric_price']*row['sum']  #设备用电费用

            #公摊分摊
            shareCost = (electCost - equipTotalCost)*float(getRatio(ymd,productId))

            finalCost = (waterCost+gasCost)*float(getRatio(ymd,productId)) + equipTotalCost + shareCost

            insertCost = '''
                update cloudteam_data_warehouse.dw_production_cost set cost= %s
                where ymd = '%s' and product_id = '%s' and dimension = '能源费'
            ''' %(finalCost/proSum,ymd,productId)
            cursor.execute(insertCost)


        else:
            continue

        conn.commit()




























