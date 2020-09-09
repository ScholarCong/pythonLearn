# coding:utf-8
import pymysql
import os
os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'
conn = pymysql.connect(host='10.20.3.7', user='root', password='Isysc0re', port=23306, db='cloudteam_data_warehouse')
cursor = conn.cursor()
sql = '''
insert into ST_PRODUCT_COST
(ymd,workshop_id,workshop,product_id,product_name,type,cost)
select task.ymd,task.workshop_id,task.workshop,task.product_id,task.product_name,'设备折旧费' as type,actural_pass_num*round(sno/60,2)*ifnull(cost,0)/total_stand_time as cost from ST_PRODUCTION_TASK task left join
    (
    select distinct production_id,pl_id from cloudteam.isyscore_pl_production
    ) pl
    on pl.production_id = task.product_id
          left join dw_product_info p on p.product_id = task.product_id
#  当月当产线下所有设备的平均折旧费
    # 折旧表以月为单位，需要除上班天数，从ST_PRODUCTION_CALENDAR 获取
left join
(select ym,pl_id,round(sum(cost_per_day),2) as cost from
(select * ,charge/sum_days as cost_per_day from ST_DEVICE_DEPRECIATION re
left join (
    select count(*) as sum_days,substr(date_format(ymd,'%Y-%m-%d'),1,7) as ym from ST_PRODUCTION_CALENDAR
group by substr(date_format(ymd,'%Y-%m-%d'),1,7)
    ) calen
on calen.ym = substr(date_format(re.ymd,'%Y-%m-%d'),1,7)
left join
    (select pl_id,equipment_num,eq.id as eq_id from cloudteam.isyscore_pl_sequence_equipment seq
        left join cloudteam.isyscore_equipement_info eq on eq.asset_code = seq.equipment_num where equipment_num is not null  ) sq
on re.device_id = sq.eq_id where device_type = '设备') pl_cost
group by ym,pl_id) t
on t.ym = substr(date_format(task.ymd,'%Y-%m-%d'),1,7) and t.pl_id = pl.pl_id
left join (select ymd,pl_id,sum(actural_pass_num*round(sno/60,2)) as total_stand_time
from ST_PRODUCTION_TASK task left join dw_product_info p on p.product_id = task.product_id
left join  (
    select distinct production_id,pl_id from cloudteam.isyscore_pl_production
    ) pl2 on pl2.production_id = task.product_id
    group by ymd,pl_id) total
on total.ymd = task.ymd and total.pl_id = pl.pl_id
having cost!=0 and cost is not null
'''
cursor.execute(sql)
conn.commit()