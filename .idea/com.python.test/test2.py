import pymysql



def main():
    username = 'aa'
    if username == 'aa':
        print('哈哈')

    if username == 'aa':
        print('true')
    else:
        print('false')

    for i in range(10):
        print('----->',i)

    num = 0
    while num <= 10:
        print('+++++>',num)
        num = num + 1


    # 列表 []
    array = ['1',1]
    print(len(array))
    array.append('ada')
    print(array)

    #元组 tuple
    t1 = ('s','1')
    print(type(t1))

    array = [1,2,3,4,66,77,7]
    t2 = tuple(array)
    print(max(t2))

if __name__ == '__main__':
    main()



