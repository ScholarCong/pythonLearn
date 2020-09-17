# coding:utf-8
import pymysql
import numpy as np
import pandas as pd
import json
from utils_p import string_datetime, return_n_s
from datetime import datetime
from functools import reduce

conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=63306,
                       db='cloudteam_data_warehouse', cursorclass=pymysql.cursors.DictCursor)  # 使用字典游标查询)

with conn.cursor() as cursor:

    insertSql = '''
            insert into dw_production_cost 
            (ymd,product_name,product_id,dimension,sum)
            select 
                record.ymd,
                record.product_name,
                record.product_id,
                0,
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
    cursor.execute(insertSql)
    # dimension = 0 : 物料费
    selectPro = '''
            select product_id from dw_production_cost 
            where ymd = date_format(now(),'%Y-%m-%d') and dimension = 0
        '''
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
                where product_id = %s
            '''
        cursor.execute(insertCost, [cost, productId])

    conn.commit()
