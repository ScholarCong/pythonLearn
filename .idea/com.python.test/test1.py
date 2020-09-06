import pymysql  ##引入依赖包


def main():
    connect = pymysql.connect(host='127.0.0.1', port=3306, user='root',
                              password='cong', db='my_test', charset='utf8')

    # 插入操作
    try:
        with connect.cursor() as cursor:
            # 拿到游标后就可以执行sql
            result = cursor.execute("insert into employee values (11,'王聪a',123,'dada@qq.com');")
            if result == 1:
                print("插入成功！")

            connect.commit()  # 需要提交事务
    except: pymysql.MySQLError:  connect.rollback()


    # 更新操作
    id = int(input("输入id"))
    lastName = input("输入名称")
    with connect.cursor() as cursor2:
        cursor2.execute('update employee set id = %s ,last_name = %s',(id,lastName))
    connect.commit()

    # 插叙操作
    with connect.cursor() as cursor1:
        set = cursor1.execute("select * from employee")
    connect.commit()
    # print(connect)




if __name__ == '__main__':
    main()
