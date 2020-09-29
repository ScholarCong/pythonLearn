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

with conn.cursor() as cursor:

    if (start_time == None and end_time == None):
        insertSql = '''
                insert into cloudteam_data_warehouse.dw_production_cost 
                (ymd,product_name,product_id,dimension,sum)
                select 
                    record.ymd,
                    record.product_name,
                    record.product_id,
                    '工装折旧费',
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
                         and dimension = '工装折旧费'
                     '''
        cursor.execute(deleteSql, [start_time, end_time])
        insertSql = '''
                       insert into cloudteam_data_warehouse.dw_production_cost 
                       (ymd,product_name,product_id,dimension,sum)
                       select 
                           record.ymd,
                           record.product_name,
                           record.product_id,
                           '工装折旧费',
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

    #计算产品的设备折旧费
    if (start_time == None and start_time == None):
        selectPro = '''
                   select product_id,sum from cloudteam_data_warehouse.dw_production_cost 
                   where ymd = date_format(now(),'%Y-%m-%d') and dimension = '工装折旧费'
               '''
    else:
        selectPro = '''
                          select product_id,sum,ymd from cloudteam_data_warehouse.dw_production_cost 
                          where ymd >= '%s' 
                          and ymd <= '%s'
                          and dimension = '工装折旧费'
                      ''' % (start_time, end_time)

    cursor.execute(selectPro)
    for row in cursor.fetchall():
        productId = row['product_id']
        now = datetime.now()
        ymd = now.strftime("%Y-%m-%d")

        if (start_time != None and end_time != None):
            ymd = row['ymd']

        selectProCost = '''
            select sum(cost) totalCost from cloudteam_data_warehouse.st_equip_pro_repair_cost
            where product_id = '%s' 
            and ymd = '%s' and equip_type = '2'
        ''' %(productId,ymd)

        cursor.execute(selectProCost)
        cost =  cursor.fetchone()
        totalCost = cost['totalCost']

        insertCost = '''
                        update cloudteam_data_warehouse.dw_production_cost 
                        set cost = %s  
                        where product_id = %s and dimension = '工装折旧费'
                        and ymd = %s
                    '''
        cursor.execute(insertCost, [totalCost/row['sum'], productId, ymd])

        conn.commit()




























