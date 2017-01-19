'''
本程序只是为了移动关键表文件到/data/mv_cfdata/bi/20161228类似中去
不重要的放到/data/mv_cfdata/bi/20161228_part_2中去
'''
import os
import datetime
import re
import shutil
class DB(object):

    #初始化类，共有库名，关键表，所有表
    def __init__(self, name, important_table_file, all_table_file , mv_important_file):
        self.name = name
        self.important_table_file = important_table_file
        self.all_table_file = all_table_file
        self.mv_important_file = mv_important_file

    def check_success_flag_func(self):
        # print(self.all_table_file)
        # print(os.listdir(self.all_table_file))
        success_flag = "success_%s%s" % (time_stamp, file_type)
        for l1 in os.listdir(self.all_table_file):
            if l1 == success_flag:
                return 1
        else:
            print("[%s] no success_flag [%s] ,will exit!" % (self.name, success_flag))
            exit()


    #显示所有的关键表
    def show_important_table_func(self):
        bi_important_table_list = []
        with open(self.important_table_file, "r") as file:
            for l1 in file.readlines():
                l1 = l1.strip()
                bi_important_table_list.append(l1)
            return bi_important_table_list

    #显示所有表
    def show_all_table_func(self):
        all_table_list = []
        for l1 in os.listdir(self.all_table_file):
            #这里先是限制扩展名
            if l1.endswith(file_type):
                pattern = re.compile(r'^(.+)_(\d+).(txt)$')
                match = pattern.match(l1)
                all_table_time_stamp = match.groups()[1]
                #这里又限制了必须是昨天的数据
                if all_table_time_stamp == time_stamp:
                    all_table_list.append(match.groups()[0])
                else:
                    pass
                    # print("WARNNING:\n\t[%s]库 目前有非昨天的数据文件，数据文件为：%s/%s" % (self.name, self.all_table_file, l1))
        return all_table_list

    #显示对比后共同的表，其实就是验证一下，关键表是不是真的存在对应的数据文件
    def show_compare_table_func(self):
        #这个是关键表格式化#
        important_table_list_tmp = []
        important_table_list = self.show_important_table_func()
        for l1 in important_table_list:
            l1 = l1.strip()
            important_table_list_tmp.append(l1)
        # print("关键表: ", important_table_list_tmp)

        #这个是所有表格式化#
        all_table_list_tmp = []
        all_table_list = self.show_all_table_func()
        for l2 in all_table_list:
            all_table_list_tmp.append(l2)
        # print("所有表：", all_table_list_tmp)

        #关键表yu所有表对比，输出确认后表的数据文件#
        compare_table_list = []
        for l1 in important_table_list_tmp:
            l1 = l1.strip()
            for l2 in all_table_list_tmp:
                if l1 == l2:
                    compare_table_list.append(l2)
        # print("[%s]" %(self.name), "对比后的关键表且存在的表为：", "\n\t", compare_table_list)
        return compare_table_list

    #移动关键数据文件
    def mv_important_table_file_func(self):
        print("正在移动[关键表]数据文件...")
        compare_table_list = self.show_compare_table_func()
        if compare_table_list == []:
            print("没有[关键表]数据文件，请确认是否已经移动过")
            return 1
        for l1 in compare_table_list:
            '''移动关键表数据文件'''
            #mv_cfdata/bi/20161228不存在时间目录就创建时间目录
            after_mv_important_file_location = "%s/%s" %(self.mv_important_file, time_stamp)
            if not os.path.isdir(after_mv_important_file_location):
                os.mkdir(after_mv_important_file_location)

            #cfdata/bi/下如有对比后的关键表数据文件，就可以移动了
            before_mv_important_file_location = "%s/%s_%s%s" % (self.all_table_file, l1, time_stamp, file_type)
            if os.path.isfile(before_mv_important_file_location):
                # print(before_mv_important_file_location, "存在，正在准备移动...")
                try:
                    shutil.move(before_mv_important_file_location, after_mv_important_file_location)
                    print(before_mv_important_file_location, "移动成功！！！")
                except Exception:
                    print(before_mv_important_file_location, "移动失败")
            else:
                print(before_mv_important_file_location, "不存在，出错了...")
                continue

    #移动非关键数据文件
    def mv_no_important_table_file_func(self):
        print("正在移动[非关键表]数据文件...")
        #mv_cfdata/bi/20161228_part_2不存在时间目录就创建时间目录
        after_mv_no_important_file_location = "%s/%s_part_2" %(self.mv_important_file, time_stamp)
        if not os.path.isdir(after_mv_no_important_file_location):
            os.mkdir(after_mv_no_important_file_location)


        # 为了防止客户乱传数据，用所有表与关键表对比出非关键表
        #非关键表数据文件如果存在就开始移动
        all_table_list = self.show_all_table_func()
        compare_table_list = self.show_compare_table_func()

        #找出非关键表，放到before_no_important_table_list中
        before_no_important_table_list = []
        for l1 in all_table_list:
            flag = True
            for l2 in compare_table_list:
                if l1 == l2:
                    flag = False
            if flag:
                before_no_important_table_list.append(l1)

        #非关键表的数据文件为空
        if before_no_important_table_list == []:
            print("没有[非关键表]数据文件，请确认是否已经移动过")
            return 1

        #移动关键表的数据文件
        for l3 in before_no_important_table_list:
            l3 = "%s/%s_%s%s" % (self.all_table_file, l3, time_stamp, file_type)
            print(l3)
            if os.path.isfile(l3):
                print("%s 移动到  %s 目录" % (l3, after_mv_no_important_file_location))
                try:
                    shutil.move(l3, after_mv_no_important_file_location)
                    print("移动成功！！！")
                except Exception:
                    print("%s 移动到  %s 目录，失败" % (l3, after_mv_no_important_file_location))

'''实例化两个库对像'''
bi = DB("bi", "conf/bi_important_table.list.txt", "cfdata/bi", "mv_cfdata/bi")
# bi = DB("bi", "conf/bi_important_table.list.TXT", "/data/cfdata/bi")
hairongyi = DB("hairongyi", "conf/hairongyi_important_table.list.txt", "cfdata/hairongyi", "mv_cfdata/hairongyi")
# hairongyi = DB("hairongyi", "conf/hairongyi_important_table.list.TXT", "/data/cfdata/hairongyi")

file_type = ".txt"

'''###一天前的时间'''
time_stamp = (datetime.datetime.now() + datetime.timedelta(-8))
time_stamp = time_stamp.strftime("%Y%m%d")



if __name__ == "__main__":
      bi = DB("bi", "conf/bi_important_table.list.txt", "cfdata/bi", "mv_cfdata/bi")
      bi.check_success_flag_func()

#diaoyong跑批，，调用完之后
#run auto java

#把20161228 改成20161228_part_1
#把20161228_part_2 改名20161228

#diaoyong跑批，，调用完之后
#run auto java

#把20161228_part_1所有数据移动到20161228
#rmdir 20161228_part_1

#done!