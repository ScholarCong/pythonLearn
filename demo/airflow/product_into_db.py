# coding:utf-8
import pymysql

conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=63306,
                       db='cloudteam_data_warehouse', cursorclass=pymysql.cursors.DictCursor)  # 使用字典游标查询)
cursor = conn.cursor()

with conn.cursor() as cursor:

    cursor.execute("delete from st_product_info")
    sql = '''
            insert into st_product_info (product_id,product_name,line_id) 
            select pro.id ,pro.produce_name,rel.line_id 
            from cloudteam.isyscore_produce_info pro 
            left join cloudteam.isyscore_line_product_rel rel 
            on pro.id = rel.product_id
            where pro.del_flag = '0' and rel.del_flag = '0'
        '''
    cursor.execute(sql)
    conn.commit()
