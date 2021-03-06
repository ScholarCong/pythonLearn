import pymysql  ##引入依赖包


def main():
    connect = pymysql.connect(host='127.0.0.1', port=3306, user='root',
                              password='cong', db='my_test', charset='utf8',
                              cursorclass=pymysql.cursors.DictCursor) #使用字典游标查询

    # 插入操作
    # try:
    #     with connect.cursor() as cursor:
    #         # 拿到游标后就可以执行sql
    #         result = cursor.execute("insert into employee values (11,'王聪a',123,'dada@qq.com');")
    #         if result == 1:
    #             print("插入成功！")
    #
    #         connect.commit()  # 需要提交事务
    # except: pymysql.MySQLError:  connect.rollback()
    #
    #
    # # 更新操作
    # id = int(input("输入id"))
    # lastName = input("输入名称")
    # with connect.cursor() as cursor2:
    #     cursor2.execute('update employee set id = %s ,last_name = %s',(id,lastName))
    # connect.commit()


    # 查询操作
    with connect.cursor() as cursor1:
        cursor1.execute("select id id,last_name name,gender gen ,email from employee")
        #插叙所有行
        result = cursor1.fetchall()
        for row in result:
            print(row['id'] ,row['name'] , row['gen'] , row['email'])  #默认的分割是空格
        # print(f'{row[0]} {row[1]} {row[2]} {row[3]}')
        #查询5行
        #cursor1.fetchmany(5)
        #查询一行
        #cursor1.fetchone()

    connect.commit()
    # print(connect)




if __name__ == '__main__':
    main()
