# coding:utf-8
import pymysql
import json
from datetime import datetime
import decimal
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

with conn.cursor() as cursor:

    #if (options_start['start_time'] == None and options_end['end_time'] == None):
    if (start_time == None and start_time == None):
        insertSql = '''
                insert into cloudteam_data_warehouse.dw_production_cost 
                (ymd,product_name,product_id,dimension,sum)
                select 
                    record.ymd,
                    record.product_name,
                    record.product_id,
                    '生产人员工资',
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
                         and dimension = '生产人员工资'
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
                           '生产人员工资',
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
    # dimension = 1 : 生产人工费

    # if (options_start['start_time'] == None and options_end['end_time'] == None):
    if (start_time == None and start_time == None):
        selectPro = '''
                select product_id,sum from cloudteam_data_warehouse.dw_production_cost 
                where ymd = date_format(now(),'%Y-%m-%d') and dimension = '生产人员工资'
            '''
    else:
        selectPro = '''
                       select product_id,sum,ymd from cloudteam_data_warehouse.dw_production_cost 
                       where ymd >= '%s' 
                       and ymd <= '%s'
                       and dimension = '生产人员工资'
                   '''  %(start_time,end_time)

    cursor.execute(selectPro)
    for row in cursor.fetchall():
        productId = row['product_id']
        now = datetime.now()
        ymd = now.strftime("%Y-%m-%d")

        if (start_time != None and end_time != None):
            ymd = row['ymd']

        # 查询考勤表请假的人员
        selectRecord = '''
            	select record.record_data
                from isyscore_form_record record
                left join isyscore_form_info info
                on record.form_id = info.id
                where record.del_flag = '0' and info.del_flag = '0'
                and info.form_name = '员工考勤表' 
                and date_format(record.create_time,'%s-%s-%s') = '%s'          
        '''  %('Y','m','d',ymd)
        cursor.execute(selectRecord)  #'%Y-%m-%d'
        # 请假人的id列表
        array = []
        for row1 in cursor.fetchall():
            # 获取行的json
            json_data = json.loads(row1['record_data'])
            # 获取josn的record字段，得到一个数组
            recordDataArray = list(map(lambda x: x['record'], json_data))
            if(recordDataArray[2] == '1'):
                personId = recordDataArray[0]
                array.append(personId)


        # 查询所有班组的人员,排除班组长,排除请假的人员
        selectPersons = '''
                select info.id,info.wt_id from cloudteam_data_warehouse.st_production_daily_record record
                left join cloudteam.isyscore_person_info info 
                on record.wt_id = info.wt_id
                where  info.del_flag = '0' 
				 and info.wt_role_code = %s
				 and record.product_id = %s 
				 and record.ymd = %s
        '''

        perArray = []
        cursor.execute(selectPersons,['0',productId,ymd])
        for row2 in cursor.fetchall():
            if(row2['id'] in array):
                continue
            else:
                perArray.append(row2['id'])

        personsTotalSalary = 0
        sums = 0
        # 遍历所有生产人员，计算单个人的计件薪资
        for index in range(len(perArray)):
            #记件工资 = 员工A生产产品A的工资  +  员工A生产产品A的岗位工资
            perId = perArray[index]
            #计算岗位工资
            selectPro = '''
                select seqRel.product_id,seqRel.sequence_standare_time time
                from isyscore_product_sequence_rel seqRel
                left join isyscore_wtperson_sequence_rel perRel 
                on seqRel.id = perRel.seq_pro_rel_id
                where seqRel.del_flag = '0' and perRel.del_flag = '0'
                and perRel.person_id = %s
            '''
            sumTime = 0
            cursor.execute(selectPro,[perId])
            for row3 in cursor.fetchall():
                proId = row3['product_id']

                selectPros = '''
                                    select seqRel.product_id,seqRel.sequence_standare_time time
                                    from isyscore_product_sequence_rel seqRel
                                    left join isyscore_wtperson_sequence_rel perRel 
                                    on seqRel.id = perRel.seq_pro_rel_id
                                    where seqRel.del_flag = '0' and perRel.del_flag = '0'
                                    and perRel.person_id = %s
                                    and seqRel.product_id = %s
                                '''
                cursor.execute(selectPros,[perId,proId])
                timeObj = cursor.fetchone()
                time = timeObj['time']  #标准工时

                selectSum = '''
                    select sum
                    from cloudteam_data_warehouse.dw_production_cost 
                    where ymd = %s and dimension = '生产人员工资'
                    and product_id = %s
                '''
                cursor.execute(selectSum,[ymd,proId])
                sumObj = cursor.fetchone()
                if(sumObj != None):
                    sum = sumObj['sum']
                    sumTime += (sum*time)

            selectProTime = '''
                              select seqRel.product_id,seqRel.sequence_standare_time time,seqRel.part_salary
                              from isyscore_product_sequence_rel seqRel
                              left join isyscore_wtperson_sequence_rel perRel 
                              on seqRel.id = perRel.seq_pro_rel_id
                              where seqRel.del_flag = '0' and perRel.del_flag = '0'
                                and perRel.person_id = %s
                                and seqRel.product_id = %s
                        '''
            cursor.execute(selectProTime, [perId, productId])
            Time =  cursor.fetchone()
            selectSum1 = '''
                                  select sum
                                  from cloudteam_data_warehouse.dw_production_cost 
                                  where ymd = %s
                                  and product_id = %s  and dimension = '生产人员工资'
                          '''
            cursor.execute(selectSum1, [ymd,productId])
            Sum = cursor.fetchone()
            CurrentProRatio = (Time['time'] * Sum['sum'])/sumTime #岗位工资的平摊比例
            #查询当前日历工作日天数
            selectDays = '''
                            select record.record_data
                            from isyscore_form_record record
                            left join isyscore_form_info info
                            on record.form_id = info.id
                            where record.del_flag = '0' and info.del_flag = '0'
                            and info.form_name = '生产日历' 
                                and date_format(record.create_time,'%Y-%m') =  
                            date_format(now(),'%Y-%m');  #'%Y-%m-%d %H:%i:%s'
                    '''
            cursor.execute(selectDays)
            daysJson = cursor.fetchone()
                    # 获取行的json
            days_data = json.loads(daysJson['record_data'])
                    # 获取josn的record字段，得到一个数组
            recordDataArray = list(map(lambda x: x['record'], days_data))
            record = recordDataArray[1]
            totalDays = len(str(record).split(","))  # 当前月考勤的天数
            # 获取当前人的月岗位工资
            selectPerPostSalary = '''
                    select post_salary from isyscore_person_info 
                    where id = %s
            '''
            cursor.execute(selectPerPostSalary, perId)
            postSalary = cursor.fetchone()  #当前人的月岗位工资
            #************************************************
            curDayPostSalary = postSalary['post_salary']/totalDays # 当前人的这个月的平均岗位工资

            #计算生产工资:记件工资
            cursor.execute(selectProTime, [perId, productId])
            partSalary = cursor.fetchone()
            salary = partSalary['part_salary']

            cursor.execute(selectSum1, [ymd,productId])
            Sum = cursor.fetchone()
            sums = Sum['sum']
            #***********************************************
            partSalary = salary * sums
            totalPerSalary =  curDayPostSalary + partSalary
            #最终结果相加
            personsTotalSalary += totalPerSalary

        # ******************************* 计算管理岗位的人员工资
        managePerArray = []
        cursor.execute(selectPersons, ['1', productId, ymd])
        for row4 in cursor.fetchall():
            managePerArray.append(row4['id'])

        selectManagePers = '''
                                  select record.record_data
                                  from isyscore_form_record record
                                  left join isyscore_form_info info
                                  on record.form_id = info.id
                                  where record.del_flag = '0' and info.del_flag = '0'
                                  and info.form_name = '班组长计件系数' 
                          '''
        totalManagePersonMoney = 0
        cursor.execute(selectManagePers)
        for row5 in cursor.fetchall():
            manageRecordJson = row5['record_data']
            manage_per_data = json.loads(manageRecordJson)
            # 获取josn的record字段，得到一个数组
            managePerData = list(map(lambda x: x['record'], manage_per_data))
            managePerId = managePerData[0]
            proId1 = managePerData[1]
            money = managePerData[2]
            if(managePerId in managePerArray):
                 cursor.execute(selectSum, [ymd, proId1])
                 sumObj1 = cursor.fetchone()
                 sum2 = sumObj1['sum']
                 totalManagePersonMoney += (sum2*money)
            else:
                continue

        totalManagePersonMoney = decimal.Decimal(totalManagePersonMoney)
        personsTotalSalary = personsTotalSalary+totalManagePersonMoney
        if(sums != 0):
            perProductionSalary = personsTotalSalary/sums
            # 每个人的生产人工费入库
            insertCost = '''
                update cloudteam_data_warehouse.dw_production_cost 
                set cost = %s  
                where product_id = %s and dimension = '生产人员工资'
                and ymd = %s
            '''
            cursor.execute(insertCost,[perProductionSalary,productId,ymd])
        conn.commit()















