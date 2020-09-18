# coding:utf-8
import pymysql
import json
from datetime import datetime
import decimal

conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=63306,
                       db='cloudteam', cursorclass=pymysql.cursors.DictCursor)  # 使用字典游标查询)

with conn.cursor() as cursor:
    insertSql = '''
            insert into cloudteam_data_warehouse.dw_production_cost 
            (ymd,product_name,product_id,dimension,sum)
            select 
                record.ymd,
                record.product_name,
                record.product_id,
                2,
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
    cursor.execute(insertSql)

    # dimension = 2 : 间接人工费
    selectPro = '''
               select product_id,sum from cloudteam_data_warehouse.dw_production_cost 
               where ymd = date_format(now(),'%Y-%m-%d') and dimension = 1
           '''
    cursor.execute(selectPro)
    for row in cursor.fetchall():
        productId = row['product_id']
        now = datetime.now()
        nowDate = now.strftime("%Y-%m-%d")

        # 查询考勤表请假的人员
        selectRecord = '''
                        select record.record_data
                        from isyscore_form_record record
                        left join isyscore_form_info info
                        on record.form_id = info.id
                        where record.del_flag = '0' and info.del_flag = '0'
                        and info.form_name = '员工考勤表' 
                        and date_format(record.create_time,'%Y-%m-%d') =  
                        date_format(now(),'%Y-%m-%d');                      #'%Y-%m-%d %H:%i:%s'
                '''
        cursor.execute(selectRecord)
        # 请假人的id列表
        array = []
        for row1 in cursor.fetchall():
            # 获取行的json
            json_data = json.loads(row1['record_data'])
            # 获取josn的record字段，得到一个数组
            recordDataArray = list(map(lambda x: x['record'], json_data))
            if (recordDataArray[2] == '1'):
                personId = recordDataArray[0]
                array.append(personId)

        # 查询所有班组的人员,排除班组长,排除请假的人员
        selectPersons = '''
              select info.id from isyscore_work_team_info wt 
              left join isyscore_person_info info on wt.id = info.wt_id 
              where wt.del_flag = '0' and info.del_flag = '0'
              and wt.wt_type = '2'
        '''

        perArray = []
        cursor.execute(selectPersons)
        for row2 in cursor.fetchall():
            if (row2['id'] in array):
                continue
            else:
                perArray.append(row2['id'])

        peopleTotalSalary = 0
        # 遍历所有生产人员，计算单个人的计件薪资 :正常考勤工资+加班工资+岗位工资
        for index in range(len(perArray)):
            personId = perArray[index]
            #正常考勤工资
            selectWtGroup = '''
                 select wt.wt_group from isyscore_work_team_info wt 
                 left join isyscore_person_info per
                 on wt.id = per.wt_id
                 where wt.del_flag = '0' and per.del_flag = '0'
                 and per.id = %s
            '''
            cursor.execute(selectWtGroup,[personId])
            groupObj = cursor.fetchone()
            groupStr = str(groupObj['wt_group'])
            workHours = 0  #正常考勤时间
            try:
                arrayTime = groupStr.split('~')
                inte = int(arrayTime[1].split(':')[0]) - int(arrayTime[0].split(':')[0])
                point = (int(arrayTime[1].split(':')[1]) - int(arrayTime[0].split(':')[1]))/60
                workHours = inte+point
            except ValueError:
                print("数据类型错误！")

            selectPerTimeSalary = '''
                 select time_salary,post_salary from isyscore_person_info 
                 where id = %s 
            '''
            cursor.execute(selectPerTimeSalary,[personId])
            timeSalary = cursor.fetchone()
            tSalary = timeSalary['time_salary'] #计时工资
            #*********************正常考勤工资
            normalSalary = decimal.Decimal(workHours)*tSalary
            #加班工资
            cursor.execute(selectRecord)
            for row3 in cursor.fetchall():
                jsonData = json.loads(row3['record_data'])
                attendList = list(map(lambda x: x['record'], jsonData))
                attendStatus = attendList[2]    #考勤状态

                selectDiyValue = '''
                     select diy_label from isyscore_form_field_config
                     where diy_value = %s  
                '''
                cursor.execute(selectDiyValue,[attendStatus])
                lable = cursor.fetchone()
                strLabel = str(lable['diy_label'])
                perId = attendList[0]
                if(strLabel == "加班" and personId == perId):
                    overNum = attendList[4]   #加班系数
                    overHours = attendList[5]   #加班小时数
                    normalSalary += (overHours*tSalary*overNum)
                    break
                else:
                    continue

            #岗位工资
            postSalary =  timeSalary['post_salary'] #月岗位工资
            # 查询当前日历工作日天数
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
            dayPostSalary = postSalary/totalDays  # 日岗位工资
            # 当前人的工资
            normalSalary += dayPostSalary
            peopleTotalSalary += normalSalary

        print(peopleTotalSalary)
        # 计算分摊规则2
        selectPros = '''
                select 
                    record.product_id,
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
            cursor.execute(selectTotalTime,productIdcur)
            standardTime = cursor.fetchone()
            totalTime = standardTime['standard_time']
            if(productId == productIdcur):
                currenctProInfo.append(totalTime)
                currenctProInfo.append(sum)

            if(totalTime != None):
                sumTime += (sum*totalTime)
        #平摊比例
        currentProRatio = (currenctProInfo[0]*currenctProInfo[1])/sumTime
        peopleTotalSalary *= currentProRatio
        #******************************************单件产品的间接人工费
        finalPerProSalary = peopleTotalSalary/currenctProInfo[1]
        #入库
        insertCost = '''
                   update cloudteam_data_warehouse.dw_production_cost 
                   set cost = %s  
                   where product_id = %s and dimension = 2
               '''
        cursor.execute(insertCost, [finalPerProSalary, productId])
        conn.commit()






























