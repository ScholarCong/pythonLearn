# coding:utf-8
import pymysql
import pandas as pd
import json
from datetime import datetime

conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=63306, db='cloudteam'
                       ,cursorclass=pymysql.cursors.DictCursor)

with conn.cursor() as cursor:

    record_sql = '''
        select record.wt_id,wt.wt_name,record.record_data
		from isyscore_form_record record
        left join isyscore_form_info info
		on record.form_id = info.id
		left join isyscore_work_team_info wt 
		on wt.id = record.wt_id
        where record.del_flag = '0' and info.del_flag = '0' and wt.del_flag = '0'
        and info.form_name = '生产日报' 
        and date_format(record.create_time,'%Y-%m-%d') = date_format(now(),'%Y-%m-%d');  #'%Y-%m-%d %H:%i:%s'
    '''
    cursor.execute(record_sql)
    for row in cursor.fetchall():
        #获取行的json
        json_data = json.loads(row['record_data'])
        #获取josn的record字段，得到一个数组
        record_data = list(map(lambda x: x['record'], json_data))

        selectPro = '''
            select produce_name,id from isyscore_produce_info where id = %s and del_flag = '0'
        '''
        cursor.execute(selectPro,[record_data[0]])
        product = cursor.fetchone()
        productName = product['produce_name']
        proId = product['id']

        #查询产品的最后一道工序
        selectProSequence = '''
            select rel.id,rel.sequence_id,seq.sequence_num
            from isyscore_product_sequence_rel rel
            left join isyscore_technology_sequence seq 
            on rel.sequence_id = seq.id 
            where rel.del_flag = '0' and seq.del_flag = '0'
            and rel.product_id = %s
            order by seq.sequence_num desc
        '''
        cursor.execute(selectProSequence,[proId])
        wtId = row['wt_id']
        for row1 in cursor.fetchall():
            lastProId = row1['id']
            break

         #查询这个班组的对应的这个产品是所属的工序
        selectWtPerSeqIds = '''
            select distinct rel.seq_pro_rel_id seqProRelId from  isyscore_person_info info 
            left join isyscore_wtperson_sequence_rel rel on rel.person_id = info.id  
            left join isyscore_product_sequence_rel prorel on prorel.id = rel.seq_pro_rel_id
            where rel.del_flag = '0' and info.del_flag = '0' 
			and prorel.del_flag = '0'
			and prorel.product_id = %s
			and info.wt_id = %s
        '''
        isLast = None
        cursor.execute(selectWtPerSeqIds,[proId,wtId])
        for row2 in cursor.fetchall():
            if (row2['seqProRelId'] == lastProId):
                isLast = 1
                break

        # 查询该班组是否为辅助班组
        selectWtType = '''
            select wt_type from isyscore_work_team_info
            where id = %s 
        '''
        cursor.execute(selectWtType,wtId)
        wtType = cursor.fetchone()  # 1:生产型  2：辅助型

        insertSql = '''
            insert  cloudteam_data_warehouse.st_production_daily_record
            values
            (
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s 
            )
        '''
        # 查询当前班组是否为产品落地的班组   1：是产品落地的班组
        now = datetime.now()
        nowDate = now.strftime("%Y-%m-%d")

        cursor.execute(insertSql,[ nowDate,row['wt_id'],row['wt_name'],wtType,record_data[0],productName,record_data[3]
                                   ,record_data[4],record_data[5],record_data[6],isLast])
        conn.commit()
