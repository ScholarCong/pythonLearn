import pymysql


def main():
    conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=53306,
                           db='cloudteam_data_warehouse', cursorclass=pymysql.cursors.DictCursor)  # 使用字典游标查询

    with conn.cursor() as cursor:
        cursor.execute("delete from dw_product_info")

        sql = ''' select 
                    info.id,
                    info.produce_name `name`,
                    rel.line_id lineId 
                    from cloudteam.isyscore_produce_info info 
                    left join cloudteam.isyscore_line_product_rel rel
                    on info.id = rel.product_id 
                    where info.del_flag = '0' and rel.del_flag = '0' '''
        cursor.execute(sql)

        for row in cursor.fetchall():
            selectSql = ''' 
                                select sum(rel.sequence_standare_time) sno from cloudteam.isyscore_product_sequence_rel rel 
                                where rel.product_id = %s and rel.sequence_id in (
                                    select seq.id from cloudteam.isyscore_technology tech  
                                    left join cloudteam.isyscore_technology_sequence seq on tech.id = seq.technology_id
                                    where tech.del_flag = '0' and seq.del_flag = '0'
			                        and tech.line_id = %s )
			                        and rel.del_flag = '0'
			                '''
            proId = row['id']
            lineId = row['lineId']
            cursor.execute(selectSql, [proId, lineId])
            result = cursor.fetchall()
            sno = int(result[0]['sno'])
            # print(sno)
            insertSql = '''
                                insert into dw_product_info (product_id,product_name,product_line,sno)
                                values (%s, %s, %s, %s)
                            '''
            cursor.execute(insertSql, [proId, row['name'], lineId, sno])

        conn.commit()


if __name__ == '__main__':
    main()
