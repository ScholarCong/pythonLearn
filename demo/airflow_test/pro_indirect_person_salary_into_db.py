# coding:utf-8
import pymysql
import json
from datetime import datetime
import decimal
import configparser
import time

conn = pymysql.connect(host='10.20.5.3', user='root', password='Isysc0re', port=63306,
                       db='cloudteam', cursorclass=pymysql.cursors.DictCursor)  # 使用字典游标查询)

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


def getAttendPerson():
    selectDialyInfo = '''
               select record.record_data
                               from isyscore_form_record record
                               left join isyscore_form_info info
                               on record.form_id = info.id
                               where record.del_flag = '0' and info.del_flag = '0'
                               and info.form_name = '员工考勤表' 
                         limit 0,1
         '''
    cursor.execute(selectDialyInfo)
    resultOne = cursor.fetchone()
    result = resultOne['record_data']
    jsonData = json.loads(result)
    recordDataArray = list(map(lambda x: x['fieldId'], jsonData))
    timeFieldId = str(recordDataArray[1])
    return timeFieldId


def getAttendDays(ymdd):
    # 查询当前日历工作日天数
    selectDays = '''
                              select record.record_data
                              from isyscore_form_record record
                              left join isyscore_form_info info
                              on record.form_id = info.id
                              where record.del_flag = '0' and info.del_flag = '0'
                              and info.form_name = '生产日历' 
                      '''
    cursor.execute(selectDays)
    daysJson = cursor.fetchall()
    for row in daysJson:
        # 获取行的json
        days_data = json.loads(row['record_data'])
        # 获取josn的record字段，得到一个数组
        recordDataArray = list(map(lambda x: x['record'], days_data))
        dateNum = int(recordDataArray[0])
        timeArray = time.localtime(dateNum / 1000)
        ymd = time.strftime("%Y-%m", timeArray)
        if (ymd == ymdd):
            totalDays = len(str(recordDataArray[1]).split(","))  # 当前月考勤的天数
            return totalDays


with conn.cursor() as cursor:
    if (start_time == None and start_time == None):
        insertSql = '''
                insert into cloudteam_data_warehouse.dw_production_cost 
                (ymd,product_name,product_id,dimension,sum)
                select 
                    record.ymd,
                    record.product_name,
                    record.product_id,
                    '间接人工费',
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
                              and dimension = '间接人工费'
                          '''
        cursor.execute(deleteSql, [start_time, end_time])
        insertSql = '''
                       insert into cloudteam_data_warehouse.dw_production_cost 
                       (ymd,product_name,product_id,dimension,sum)
                       select 
                           record.ymd,
                           record.product_name,
                           record.product_id,
                           '间接人工费',
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

    # dimension = 2 : 间接人工费
    if (start_time == None and start_time == None):
        selectPro = '''
                   select product_id,sum,ymd from cloudteam_data_warehouse.dw_production_cost 
                   where ymd = date_format(now(),'%Y-%m-%d') and dimension = '间接人工费'
               '''
    else:
        selectPro = '''
                          select product_id,sum,ymd from cloudteam_data_warehouse.dw_production_cost 
                          where ymd >= '%s' and ymd <= '%s'
                          and dimension = '间接人工费'
                      ''' % (start_time, end_time)

    cursor.execute(selectPro)
    for row in cursor.fetchall():
        productId = row['product_id']
        now = datetime.now()
        nowDate = now.strftime("%Y-%m-%d")

        ymd = str(row['ymd'])
        # 查询考勤表请假的人员
        ymdNum = int(time.mktime(time.strptime(ymd, "%Y-%m-%d"))) * 1000
        selectRecord = '''
                   select record_data from isyscore_form_record record
                   where id in 
                   (
                   select search_id from isyscore_form_seach_index search
                   where field_id = '%s' and field_data = %s
                   and search.del_flag = '0'
                   )
                   and record.del_flag = '0'  
               ''' % (getAttendPerson(), ymdNum)
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
            # 正常考勤工资
            selectWtGroup = '''
                 select wt.wt_group from isyscore_work_team_info wt 
                 left join isyscore_person_info per
                 on wt.id = per.wt_id
                 where wt.del_flag = '0' and per.del_flag = '0'
                 and per.id = %s
            '''
            cursor.execute(selectWtGroup, [personId])
            groupObj = cursor.fetchone()
            groupStr = str(groupObj['wt_group'])
            workHours = 0  # 正常考勤时间
            try:
                arrayTime = groupStr.split('~')
                inte = int(arrayTime[1].split(':')[0]) - int(arrayTime[0].split(':')[0])
                point = (int(arrayTime[1].split(':')[1]) - int(arrayTime[0].split(':')[1])) / 60
                workHours = inte + point
            except ValueError:
                print("数据类型错误！")

            selectPerTimeSalary = '''
                 select time_salary,post_salary from isyscore_person_info 
                 where id = %s 
            '''
            cursor.execute(selectPerTimeSalary, [personId])
            timeSalary = cursor.fetchone()
            tSalary = timeSalary['time_salary']  # 计时工资
            # *********************正常考勤工资
            normalSalary = decimal.Decimal(workHours) * tSalary
            # 加班工资
            cursor.execute(selectRecord)
            for row3 in cursor.fetchall():
                jsonData = json.loads(row3['record_data'])
                attendList = list(map(lambda x: x['record'], jsonData))
                attendStatus = attendList[2]  # 考勤状态

                selectDiyValue = '''
                     select diy_label from isyscore_form_field_config
                     where diy_value = %s  
                '''
                cursor.execute(selectDiyValue, [attendStatus])
                lable = cursor.fetchone()
                strLabel = str(lable['diy_label'])
                perId = attendList[0]
                if (strLabel == "加班" and personId == perId):
                    overNum = attendList[4]  # 加班系数
                    overHours = attendList[5]  # 加班小时数
                    normalSalary += (overHours * tSalary * overNum)
                    break
                else:
                    continue

            # 岗位工资
            postSalary = timeSalary['post_salary']  # 月岗位工资
            ymdArray = ymd.split("-")
            ym = str(ymdArray[0]) + "-" + str(ymdArray[1])
            totalDays = getAttendDays(ym)  # 当前月考勤的天数

            dayPostSalary = postSalary / totalDays  # 日岗位工资
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

            if (totalTime != None and sum != None):
                sumTime += (sum * totalTime)
        # 平摊比例
        currentProRatio = (currenctProInfo[0] * currenctProInfo[1]) / sumTime
        peopleTotalSalary *= currentProRatio
        # ******************************************单件产品的间接人工费
        finalPerProSalary = peopleTotalSalary / currenctProInfo[1]
        # 入库
        insertCost = '''
                   update cloudteam_data_warehouse.dw_production_cost 
                   set cost = %s  
                   where product_id = %s and dimension = '间接人工费'
                   and ymd = %s
               '''
        cursor.execute(insertCost, [finalPerProSalary, productId, ymd])
        conn.commit()
