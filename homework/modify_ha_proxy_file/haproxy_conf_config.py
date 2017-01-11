import json

#查询所有的backend记录
def serach_backend_func():
    with open(file, "r+") as file_ha_conf:
        file_ha_conf_list = file_ha_conf.readlines()  ##把文件内容传入一个大列表,这个目前是必须的,不然比较的时候会有错
        backend_list = []  # 初始化backend列表
        for line in file_ha_conf_list:  # 开始找所有的backend
            line = line.strip()
            if line.startswith("backend"):  # 找到backend的操作
                line = line.split(" ")[1]
                backend_list.append(line)  # 把backend保存起来
    return backend_list

#查询指定backend下的server记录（需传入一个backend参数）
def serach_backend_server_func(custom_input_backend_list="bbb.oldboy.com"):  ###查询backend 木块
    # custom_input_backend_list = "buy.oldboy.org"
    with open(file, "r+") as file_ha_conf:
        insert_backend_flag = False
        insert_backend = []
        file_ha_conf_list = file_ha_conf.readlines()
        for line in file_ha_conf_list:
            line = line.strip()
            str_custom_input_backend_list = str(custom_input_backend_list)  ###转换格式成str 而后才可以 ==
            str_custom_input_backend_list = str_custom_input_backend_list.strip()
            if ("backend %s" % (str_custom_input_backend_list)) == line:
                insert_backend_flag = True
                insert_backend.append(line)
                continue
            if insert_backend_flag and line.startswith("server"):
                insert_backend.append(line)
            else:
                insert_backend_flag = False
    # print(insert_backend)
    return insert_backend


def add_backend_and_server_func():
    # print("请直接输入你要输入的backend，如是新的backend，也可直接输入")
    custom_input_backend_list = input("请直接输入你要输入的backend，如是新的backend，也可直接输入:\n")
    #"bbs.oldboy.org"  ###客户输入
    backend_list = serach_backend_func()
    for line in backend_list:
        if line == custom_input_backend_list:
            print("backend已经存在,请输入添加的server信息")
            custom_input_server_list = input("\n")
            # custom_input_server_list = "server qqq2 100.1.7.10 100.1.7.10 weight 10 maxconn 2000"  ###客户输入
            add_server_func(custom_input_backend_list, custom_input_server_list)
            break
    else:
        print("backend不存在,确定要添加吗？:")
        add_backend_func(custom_input_backend_list)


def add_server_func(custom_input_backend_list, custom_input_server_list):
    backend_server_list = serach_backend_server_func(custom_input_backend_list)
    backend_server_list.append(custom_input_server_list)

    print(backend_server_list)
    write_output_file_func(backend_server_list,custom_input_backend_list)



def add_backend_func(custom_input_backend_list):
    backend_list = serach_backend_func()
    default_backend_first = str(backend_list[0])  ###默认第一个banckend块
    backend_server_list = serach_backend_server_func(default_backend_first)
    custom_input_backend_list = "backend %s " % (custom_input_backend_list)
    backend_server_list.append(custom_input_backend_list)
    print(backend_server_list)
    write_output_file_func(backend_server_list)

def delete_backend_and_server_func():
    ##客户选择要删除backend
    custom_input_delete_choise = "backend"

    custom_input_delete_backend = "buy.oldboy.org"
    # custom_input_delete_backend = input("输入你想删除的backend:")
    delete_backend_func(custom_input_delete_choise, custom_input_delete_backend)


def delete_backend_func(custom_input_delete_choise, custom_input_delete_backend):
    backend_list = serach_backend_func()
    print("你的选择是删除", custom_input_delete_choise)
    for line in backend_list:
        if line == custom_input_delete_backend:
            print("backend已经存在,是否删除整个backend")
            # 是,删除整个backend
            # print("你的选择是删除整个backend")
            # backend_server_list = serach_backend_server_func(custom_input_delete_backend)
            # print(backend_server_list)


            # 否,只删除backend某一个server
            print("你的选择是只删除backend某一个server")
            print("请输入你要选择删除的serer")
            backend_server_list = serach_backend_server_func(custom_input_delete_backend)
            print('原来是:', backend_server_list)
            custom_input_delete_server = "server buy2 100.1.7.90 100.1.7.90 weight 20 maxconn 3000"
            print("你删除的是:", custom_input_delete_server)
            backend_server_list.remove(custom_input_delete_server)
            print("最后结果是:", backend_server_list)
            break
    else:
        print("backend不存在,请重新输入要删除的backend")
        # delete_backend_and_server_func()


###--------------------修改模块------------------------------------#####
##修改文件内容,修改backend名称,以及修改backend下的server参数
def modify_backend_and_server_func():
    ###---------------------修改模块之修改backend的名称----------------------------#####
    print("你想修改什么,修改backend的名称,还是修改修改backend下的server参数", "\n")
    print("============================================")
    print('''   你的选择是
    1.修改backend的名称
    2.修改backend下的server参数
    ''')
    print("============================================")
    custom_input_choise = int(input("\n"))

    if custom_input_choise == 1:
        # 判断,选择1,我想修改 backend的名称
        print("哦,想改backend,你要修改哪个backend的名字")

        backend_list = serach_backend_func()        ###查出所有的backend列表,告诉客户这是可以修改的当前backend列表
        print(backend_list)
        custom_input_modify_backend_chiose = input("")
        # custom_input_modify_backend_chiose = "buy.oldboy.org"  ###客户选择要修改的backend名称


        if custom_input_modify_backend_chiose in backend_list:  ###判断客户选择的backend在 backend列表里
            custom_input_modify_backend = input("请问你要改成啥")
            # custom_input_modify_backend = "buy2.oldboy.org"    ###客户输入,他想要修改后的backend名字
            print("你修改的选项是:",custom_input_modify_backend_chiose,"修改成",custom_input_modify_backend,"\n")
            backend_server_list = serach_backend_server_func(custom_input_modify_backend_chiose)   ####查询出这个backend,以及他下面的server的所有记录

            ###把客户选择要修改掉的backend名字,在backend块中先抹掉
            custom_input_modify_backend_choise = "backend %s" % custom_input_modify_backend_chiose
            backend_server_list.remove(custom_input_modify_backend_choise)

            ##把客户要修改后backend名字,放到backend块
            backend_server_list.insert(0,custom_input_modify_backend)
            write_output_file_func(backend_server_list,custom_input_modify_backend_chiose)

        ##客户选择的backend名称不在backend列表内
        else:
            print("名称不存在,请输入正确的banckend名称")

    elif custom_input_choise == 2:
        ##修改文件内容,修改backend下的server参数
        print("哦,你想要修改server,你要在哪个backend下修改")
        backend_list = serach_backend_func()  ###输出所有的backend列表,告诉客户这是可以修改的当前backend列表
        print(backend_list, "\n")
        custom_input_modify_backend_chiose = "bbb.oldboy.org"  ###客户选择要修改的backend名称
        custom_input_modify_backend_chiose = input("你要选择的backend名称，改backend下修改的server：\n")

        print("你要修改的是:", custom_input_modify_backend_chiose, "\n")
        if custom_input_modify_backend_chiose in backend_list:  ###判断客户选择的backend在 backend列表里
            ##选择对应的server中一条记录
            backend_server_list = serach_backend_server_func(custom_input_modify_backend_chiose)  ##查询对应server记录
            # print("目前这个backend块记录是:", )
            # print(backend_server_list, "\n")

            print("请选择你要修改的server信息是第几条")
            print("当前backend块为:", custom_input_modify_backend_chiose)
            for line in range(1, len(backend_server_list)):
                print(line, backend_server_list[line], "")

            print("\n")
            custom_input_modify_server_chiose = int(input("请选择修改那一条server记录:\n"))

            print("你选择的是第", custom_input_modify_server_chiose, "条:", backend_server_list[custom_input_modify_server_chiose],
                  "\n")

            ###删除对应的server中特定记录,添加用户输入的记录
            for line in range(1, len(backend_server_list)):
                if custom_input_modify_server_chiose == line:
                    print("你要修改的记录,当前为:", "\n", backend_server_list[line], "\n")
                    ###客户输入对应修改的server信息
                    custom_input_modify_server = input("请输入你要修改成啥：\n")
                    # custom_input_modify_server = "server bbb2 100.1.7.9 100.1.7.9 weight 20 maxconn 9999"
                    print("你要修改的记录:",backend_server_list[line],"修改后为：",custom_input_modify_server)
                    backend_server_list.remove(backend_server_list[line])
                    backend_server_list.insert(custom_input_modify_server_chiose, custom_input_modify_server)
                    print("修改后的信息为", backend_server_list)

                    write_output_file_func(backend_server_list,custom_input_modify_backend_chiose)


def write_output_file_func(arg1,arg2=""):
    with open("ha01.conf", "r") as file_ha_conf, open("ha01.test.txt", "w+") as output_ha_conf:
        file_ha_conf_list = file_ha_conf.readlines()
        output_ha_conf_list = output_ha_conf.readlines()
        arg2 = "backend %s" %(arg2)

        print(arg2)
        no_input_flag = False
        ori_input_flag = False

        for line in file_ha_conf_list:
            # print(line)
            line2 = line.strip()
            if line2 == arg2:
                no_input_flag = True
                for L1 in arg1:
                    L1 = str(L1)
                    if not L1.startswith("server"):
                        output_ha_conf.writelines(L1)
                        output_ha_conf.writelines("\n")
                    else:
                        output_ha_conf.writelines("\t\t")
                        output_ha_conf.writelines(L1)
                        output_ha_conf.writelines("\n")
                    continue
            if no_input_flag and line2.startswith("server"):
                continue
            else:
                if line2 == arg2:
                    ori_input_flag = True
                    continue
                if ori_input_flag and line2.startswith("server"):
                    continue
                else:
                    ori_input_flag = False
                no_input_flag = False

            output_ha_conf.writelines(line)

def customs_input_func():
    customs_input = input("请输入你想输入的内容：\n")
    #{"server":"100.1.7.9","weight":"20","maxconn":"3000"}
    #{"backend":"ttt.oldboy.org","record":{"server":"100.1.7.9","weight":"20","maxconn":"3000"}}
    customs_input = json.loads(customs_input)
    return customs_input


def menu():
    print("============================================")
    print('\033[33m 欢迎访问haproxy配置文件管理平台：\033[0m')
    print('''   HAproxy配置管理器
    1.查询记录
    2.新增记录
    3.修改记录
    4.删除记录
    5.退出''')
    print("============================================")

    custom_choise = int(input("请输入你的选择:"))
    if custom_choise == 1:
        print("你选择的是查询记录，目前系统中的backend记录有：")
        backend_list = serach_backend_func()
        print(backend_list) ###文件中所有的backend 信息,展示模块
        custom_search_backend_input = input("请输入你要查询的banckend")
        # print("你输入的backend是：bbb.oldboy.org")
        backend_server_list = serach_backend_server_func(custom_search_backend_input)
        print(backend_server_list)

    elif custom_choise == 2:
        print("你选择的是新增记录")
        add_backend_and_server_func()
    elif custom_choise == 3:
        print("修改模块")
        modify_backend_and_server_func()
    elif custom_choise == 4:
        delete_backend_and_server_func()
        print("4")
    elif custom_choise == 5:
        exit()


if __name__ == '__main__':
    file = "ha01.conf"
    other_file2 = "ha01.test.txt"
    menu()


    # 客户输入模块
    # serach_backend_server_func()  ###查询模块
    #  ###添加模块
    # delete_backend_and_server_func() ##删除模块
    # aaa = modify_backend_and_server_func()  ##修改模块

