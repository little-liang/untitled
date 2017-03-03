import cx_Oracle, sys, datetime, time, subprocess, os
import multiprocessing

'''自动跑批类'''


class auto_java_DB_class(object):
    def __init__(self, name, rename, sql):
        self.name = name
        self.rename = rename
        self.sql = sql

    def check_custom_upload_data_func(self):
        uptate_table = self.sql.search_sql_func()
        update_table_time = datetime.datetime.strptime(uptate_table[1], "%Y%m%d") + datetime.timedelta(days=1)
        update_table_time = update_table_time.strftime("%Y%m%d")

        # mv_cfdata_dir = "mv_cfdata"
        mv_cfdata_dir = "/data/mv_cfdata"
        mv_cfdata_dir_success_file = "%s/%s/%s/success_%s.TXT" % (
        mv_cfdata_dir, self.rename, update_table_time, update_table_time)

        # if os.path.isfile(mv_cfdata_dir_success_file):
        #    print("check the condition, The [%s] [%s] success file is exist, pass, continue...\n" % (self.name, mv_cfdata_dir))
        # else:
        #    print("The file [%s] is not exist, will exit!" % (mv_cfdata_dir_success_file))
        #    os._exit()

    '''检查客户（孙振）注意的那张表，正常跑批会更新'''

    def check_DB_update_time_table_flag_func(self, ):
        uptate_table = self.sql.search_sql_func()
        update_table_time = uptate_table[1]

        # 判断批处理是否已经完成，完成就退出
        if time_stamp == update_table_time:
            print("[%s] DB [%s] auto_java is done already!" % (self.name, update_table_time))
            os.exit()
        elif time_stamp > update_table_time:
            print("[%s] DB [%s] auto_java  will start..." % (self.name, (
            datetime.datetime.strptime(uptate_table[1], "%Y%m%d") + datetime.timedelta(days=1)).strftime("%Y%m%d")))
        else:
            print("The [%s] time [%s] is wrong, pls check ctl_fc table" % (self.name, update_table_time))
            os._exit()

    # 检查跑批进程是否正在运行
    def check_auto_java_DB_isRunning_func(self):
        uptate_table = self.sql.search_sql_func()
        update_table_time = datetime.datetime.strptime(uptate_table[1], "%Y%m%d") + datetime.timedelta(days=1)
        update_table_time = update_table_time.strftime("%Y%m%d")
        # 调用ps 命令查询相关进程
        # cmd = "netstat -n"

        cmd = "ps -ef|grep java|egrep -v 'u01|sh|py'"

        # print(cmd)
        # # 检查shell命令是否能够执行
        # try:
        #     p = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
        #     print(p.decode())
        #
        # except subprocess.CalledProcessError:
        #     print("The command [%s] wrong, will exit!" % (cmd))
        #     os._exit()
        #
        # print("shell ok!")

        ps_status = "root      21457  21450  1 11:45 pts/0    00:00:04 /usr/bin/java -Xms512m -Xmx1024m -jar /data/java_scripts/freight20161219.jar /data/java_scripts/fcsbs.properties 20170302"

        # 查跑批进程是否正在运行
        print(ps_status)
        print("%s %s" % (self.name, update_table_time))
        if ps_status.__contains__(self.name) and ps_status.__contains__(update_table_time):
            print("ok")

        os._exit()

    # 开始进行跑批工作
    def run_auto_java_DB_func(self):
        uptate_table = self.sql.search_sql_func()
        update_table_time = datetime.datetime.strptime(uptate_table[1], "%Y%m%d") + datetime.timedelta(days=1)
        update_table_time = update_table_time.strftime("%Y%m%d")

        # 这个标识用来是否更新标识表，和插入跑批数据
        auto_java_flag = False

        print("[%s] DB not auto java! will run ... " % (self.name))
        sys.stdout.flush()

        ##取跑批开始时间
        start_date_time_stamp = datetime.datetime.now()
        ## 跑批时间的秒格式
        start_date_time_stamp_second = start_date_time_stamp
        start_date_time_stamp = start_date_time_stamp.strftime("%Y/%m/%d %H:%M:%S")

        # 正式调用java程序跑批
        arg1 = "/usr/bin/java -Xms512m -Xmx1024m -jar"
        arg2 = "/server/scripts/auto_java_DB_everyday/auto_java_properties/freight20161219.jar"
        arg2 = "/data/java_scripts/freight20161219.jar"
        arg3 = "/server/scripts/auto_java_DB_everyday/auto_java_properties/"
        arg3 = "%s%s.properties" % (arg3, self.name)
        arg4 = update_table_time

        cmd = "%s %s %s %s" % (arg1, arg2, arg3, arg4)
        print(cmd)
        sys.stdout.flush()

        try:
            auto_java_flag = True
            a = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            for l1 in a.stdout.readlines():
                l1 = l1.strip()
                l1 = l1.decode()

        except Exception:
            print("can't run auto java,pls check the java command!")
            os.exit()

        # 计算出跑批完成时间
        end_date_time_stamp = (datetime.datetime.now() + datetime.timedelta(-0))
        end_date_time_stamp_second = end_date_time_stamp
        end_date_time_stamp = end_date_time_stamp.strftime("%Y/%m/%d %H:%M:%S")
        # print("[%s] auto java end time：%s" % (self.name, end_date_time_stamp))

        # 计算出跑批时间
        cost_time = end_date_time_stamp_second - start_date_time_stamp_second
        cost_time = (cost_time.seconds / 60)
        cost_time = round(cost_time, 2)

        # if auto_java_flag:
        if True:
            # 写入跑批标识标(本地mysql)
            print("写入跑批标识标(本地mysql)")
            self.sql.insert_into_sql_func(start_date_time_stamp, end_date_time_stamp, cost_time)

            # 写入跑批标识表（oracle，孙振等看）
            print("写入跑批标识表（oracle，孙振等看")
            self.sql.update_sql_func(update_table_time, end_date_time_stamp)
        else:
            print("auto java is fail, will exit!")
            os.exit()


# oracle操作类
class oracle_run_sql_calss(object):
    def __init__(self, name, oracle_DB, search_sql):
        self.name = name
        self.oracle_DB = oracle_DB
        if oracle_DB == 223:
            self.oracle_DB = "etl/etl@10.138.22.223:1521/edw"
        elif oracle_DB == 226:
            self.oracle_DB = "etl/etl_Haier1111@10.138.22.226:1521/edw"
        self.search_sql = search_sql

    # 查询标识表方法
    def search_sql_func(self):
        try:
            conn = cx_Oracle.connect(self.oracle_DB)
            cur = conn.cursor()
            search_sql = "%s where system_id = '%s'" % (self.search_sql, self.name)
            search_sql_tuple = cur.execute(search_sql).fetchone()
            conn.close()
            return search_sql_tuple
        except:
            print("Oracle connect is broken!!!")
            os.exit()

    # 更新孙振的oracle标识表
    def update_sql_func(self, data_date, update_time):
        # 拼凑sql语句更新用的
        update_sql = "update ctl_fc set data_date = '%s',update_time = to_date('%s','yyyy/mm/dd HH24:MI:SS') where system_id = '%s'" % (
        data_date, update_time, self.name)

        conn = cx_Oracle.connect(self.oracle_DB)
        cur = conn.cursor()
        cur.execute(update_sql)
        conn.commit()
        conn.close()

    # 插入更新表（自己看的，mysql），自己看的
    def insert_into_sql_func(self, start_date_time_stamp, end_date_time_stamp, cost_time):
        insert_into_sql = "insert into ctl_fc_time(system_id,start_date,end_date,Data_Date,cost_time) values"
        insert_into_sql = "%s('%s',to_date('%s','yyyy/mm/dd HH24:MI:SS'),to_date('%s','yyyy/mm/dd HH24:MI:SS'),'%s','%s')" % (
        insert_into_sql, self.name, start_date_time_stamp, end_date_time_stamp, time_stamp, cost_time)

        conn = cx_Oracle.connect(self.oracle_DB)
        cur = conn.cursor()
        cur.execute(insert_into_sql)
        conn.commit()
        conn.close()


'''###一天前的时间（数据文件时间，将要跑批时间）'''
time_stamp = (datetime.datetime.now() + datetime.timedelta(-1))
time_stamp = time_stamp.strftime("%Y%m%d")


# 外层自动跑批,由并发多进程（注意！！是多进程调用）
def auto_java_func(DB_name, DB_rename):
    # 原本单个实例化，变成由并发多进程 同时实例化同时进行批处理动作
    # fchry_sql = oracle_run_sql_calss("fchry", 226, "select * from ctl_fc")
    # fchry_auto_java = auto_java_DB_class("fchry", fchry_sql)
    # fchry_auto_java.run_auto_java_DB_func()

    # 编凑实例化，这里有两个类
    DB_sql = "%s_sql" % (DB_name)
    DB_auto_java = "%s_auto_java" % (DB_name)

    ###变量式实例化，shell爱用的峰哥
    DB_sql = oracle_run_sql_calss(DB_name, 223, "select * from ctl_fc")
    DB_auto_java = auto_java_DB_class(DB_name, DB_rename, DB_sql)

    # 检查是否已经跑过批
    # DB_auto_java.check_DB_update_time_table_flag_func()

    # 检查跑批是否正在运行
    DB_auto_java.check_auto_java_DB_isRunning_func()

    # 检查是否已经传来数据
    # DB_auto_java.check_custom_upload_data_func()

    # 真正跑批
    DB_auto_java.run_auto_java_DB_func()


# 主函数
if __name__ == '__main__':
    # conf_file = "/server/scripts/auto_java_DB_everyday/auto_java_scripts_sbs/DBlist.txt"
    # win
    conf_file = "DBlist.txt"

    # 进程池，同时8个
    pool = multiprocessing.Pool(8)

    # 读配置文件，按照数据库名进行进程池添加
    with open(conf_file, "r") as f:
        for line in f.readlines():
            line = line.strip()
            DB_name = line.split()[0]
            DB_rename = line.split()[1]
            pool.apply_async(auto_java_func, args=(DB_name, DB_rename))
    pool.close()
    pool.join()