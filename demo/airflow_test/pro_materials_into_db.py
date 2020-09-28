# coding:utf-8
import pymysql
import configparser

conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=63306,
                       db='cloudteam_data_warehouse', cursorclass=pymysql.cursors.DictCursor)  # 使用字典游标查询)

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

    if (start_time == None and start_time == None):
        insertSql = '''
                insert into dw_production_cost 
                (ymd,product_name,product_id,dimension,sum)
                select 
                    record.ymd,
                    record.product_name,
                    record.product_id,
                    '产品材料费',
                    (
                         select distinct actural_pass_num 
                         from st_production_daily_record 
                         where product_id = record.product_id
                         and is_last = 1 and ymd = date_format(now(),'%Y-%m-%d')
                     ) sum 
                from st_production_daily_record record
                where record.ymd = date_format(now(),'%Y-%m-%d')
                and record.wt_type = '1'
                group by record.product_id 
            '''
    else:
        # 删除当前天的计算数据
        deleteSql = '''
                   delete from dw_production_cost
                   where ymd >= %s and ymd <= %s 
                   and dimension = '产品材料费'
               '''

        cursor.execute(deleteSql,[start_time,end_time])
        insertSql = '''
                       insert into dw_production_cost 
                       (ymd,product_name,product_id,dimension,sum)
                       select 
                           record.ymd,
                           record.product_name,
                           record.product_id,
                           '产品材料费',
                           (
                                select distinct actural_pass_num 
                                from st_production_daily_record 
                                where product_id = record.product_id
                                and is_last = 1 and ymd = record.ymd
                            ) sum 
                       from st_production_daily_record record
                       where record.ymd >= '%s'
                       and record.ymd <= '%s'
                       and record.wt_type = '1'
                       group by record.ymd,record.product_id 
                   '''  %(start_time,end_time)

    cursor.execute(insertSql)

    # dimension = 0 : 物料费
    if (start_time == None and start_time == None):
        selectPro = '''
                       select product_id from dw_production_cost 
                       where ymd = date_format(now(),'%Y-%m-%d') 
                       and dimension = '产品材料费'
                       group by product_id
                   '''
    else:
        selectPro = '''
                select product_id from dw_production_cost 
                where ymd >= '%s' 
                and ymd <= '%s'
                and dimension = '产品材料费'
                group by product_id
            ''' %(start_time,end_time)

    cursor.execute(selectPro)
    for row in cursor.fetchall():
        productId = row['product_id']
        selectTotalPrice = '''
                select sum(bom.part_dosage*bom.bom_price) cost
                from cloudteam.isyscore_bom_info bom
                where bom.bom_brand = 
                (
                 select distinct info.bom_brand
                 from cloudteam.isyscore_produce_info info 
                 where info.id = %s
                )
                and bom.bom_version = 
                (
                         select distinct max(bom1.bom_version)
                         from cloudteam.isyscore_bom_info bom1
                         where bom1.bom_brand = 
                         (
                                 select distinct info.bom_brand
                                 from cloudteam.isyscore_produce_info info 
                                 where info.id = %s
                         )
                )
            '''
        cursor.execute(selectTotalPrice, [productId, productId])
        costObj = cursor.fetchone()
        cost = costObj['cost']
        insertCost = '''
                update dw_production_cost set cost = %s
                where product_id = %s and dimension = '产品材料费'
            '''
        cursor.execute(insertCost, [cost, productId])

    conn.commit()
