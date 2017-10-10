def find_num(data,num):
    if num <= data[len(data)-2] and num >= data[0]:        ###控制输入查询数字不要溢出
        mid = int(len(data)/2)                              ##把数组的下标分成2部分
        if len(data) >= 1:                                  ##数组小标切割至最小是1个,小标是0

            if num > data[mid]:             #待查询数字在数组中间右边
                print("right")
                find_num(data[mid:],num)        #待查询数字在数组中间左边
            elif num < data[mid]:
                print("left")
                find_num(data[:mid],num)
            else:               ##相等时
                print(data[mid])
        else:                                       #查询不到时
            print("no find")
    else:                               #超出范围时
        print("no find")



data = list(range(1,5))
find_num(data,6)
print(data)
