import cx_Oracle, sys, datetime, time, subprocess, os
import multiprocessing

'''自动跑批类'''
class auto_java_DB_class(object):
    def __init__(self, name, rename, sql):
        self.name = name
        self.rename = rename
        self.sql = sql


    def check_custom_upload_data_func(self):
        # mv_cfdata_dir = "mv_cfdata"
        mv_cfdata_dir = "/data/mv_cfdata"
        mv_cfdata_dir = "%s/%s/%s" % (mv_cfdata_dir, self.rename, time_stamp)

        print(mv_cfdata_dir)
        if len(os.listdir(mv_cfdata_dir)) == 0:
            print("The [%s] no data!, will eixt!" % (mv_cfdata_dir))
            os.exit()


    '''检查客户（孙振）注意的那张表，正常跑批会更新'''
    def check_DB_update_time_table_flag_func(self, ):
        uptate_table = self.sql.search_sql_func()
        update_table_time = uptate_table[1]

        #这里做一个时间差，超过1天(不包含一天)，就代表不能数据跑批不正常
        time_cha = abs(int(time_stamp) - int(update_table_time))
        if time_cha > 1:
            print("[%s] DB auto java was wrong!!! over 2 day data not auto java, will exit!!!")
            os.exit()
        if time_stamp <= update_table_time:
            print("[%s] DB update flag_table already!, will exit!" % (self.name))
            os.exit()
        else:
            return 1

    #检查跑批进程是否正在运行
    def check_auto_java_DB_isRunning_func(self):
        #调用ps 命令查询相关进程
        cmd = "ps aux|grep java|egrep -v 'u01|sh'|grep"
        cmd = "%s '%s'|wc -l" % (cmd, self.name)

        try:
            a = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE)
            for l1 in a.stdout.readlines():
                l1 = l1.strip()
                l1 = l1.decode()
                if l1 == '0':
                    return 1
                else:
                    print("[%s] auto java is running alreday!\n" % (self.name))
                    os.exit()
        except Exception:
            os.exit()

    #开始进行跑批工作
    def run_auto_java_DB_func(self):
        #这个标识用来是否更新标识表，和插入跑批数据
        auto_java_flag = False

        print("[%s] DB not auto java! will run ... " %(self.name))
        sys.stdout.flush()

        ##取跑批开始时间
        start_date_time_stamp = (datetime.datetime.now() + datetime.timedelta(-0))
        ## 跑批时间的秒格式
        start_date_time_stamp_second = start_date_time_stamp
        start_date_time_stamp = start_date_time_stamp.strftime("%Y/%m/%d %H:%M:%S")


        #正式调用java程序跑批
        arg1 = "java -jar -Xms512m -Xmx1024m"
        arg2 = "/server/scripts/auto_java_DB_everyday/auto_java_properties/freight20161219.jar"
        arg3 = "/server/scripts/auto_java_DB_everyday/auto_java_properties/"
        arg3 = "%s%s.properties" % (arg3, self.name)
        arg4 = time_stamp

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
            print("can't run auto java,pls check the code")

    #计算出跑批完成时间
        end_date_time_stamp = (datetime.datetime.now() + datetime.timedelta(-0))
        end_date_time_stamp_second = end_date_time_stamp
        end_date_time_stamp = end_date_time_stamp.strftime("%Y/%m/%d %H:%M:%S")
        # print("[%s] auto java end time：%s" % (self.name, end_date_time_stamp))

        #计算出跑批时间
        cost_time = end_date_time_stamp_second - start_date_time_stamp_second
        cost_time = (cost_time.seconds/60)

        if auto_java_flag:
            #写入跑批标识标(本地mysql)
            print("写入跑批标识标(本地mysql)")
            self.sql.insert_into_sql_func(start_date_time_stamp, end_date_time_stamp, cost_time)

            #写入跑批标识表（oracle，孙振等看）
            print("写入跑批标识表（oracle，孙振等看")
            self.sql.update_sql_func(time_stamp, end_date_time_stamp)
        else:
            print("auto java is fail, will exit!")
            os.exit()

#oracle操作类
class oracle_run_sql_calss(object):
    def __init__(self, name, oracle_DB, search_sql):
        self.name = name
        self.oracle_DB = oracle_DB
        if oracle_DB == 223:
            self.oracle_DB = "etl/etl@10.138.22.223:1521/edw"
        elif oracle_DB == 226:
            self.oracle_DB = "etl/etl_Haier@10.138.22.226:1521/edw"
        self.search_sql = search_sql


    #查询标识表方法
    def search_sql_func(self):
        conn = cx_Oracle.connect(self.oracle_DB)
        cur = conn.cursor()
        search_sql = "%s where system_id = '%s'" % (self.search_sql, self.name)
        search_sql_tuple = cur.execute(search_sql).fetchone()
        conn.close()
        return search_sql_tuple

    #更新孙振的oracle标识表
    def update_sql_func(self, data_date, update_time):
        #拼凑sql语句更新用的
        update_sql = "update ctl_fc set data_date = '%s',update_time = to_date('%s','yyyy/mm/dd HH24:MI:SS') where system_id = '%s'" % (data_date, update_time, self.name)

        conn = cx_Oracle.connect(self.oracle_DB)
        cur = conn.cursor()
        cur.execute(update_sql)
        conn.commit()
        conn.close()


    #插入更新表（自己看的，mysql），自己看的
    def insert_into_sql_func(self, start_date_time_stamp, end_date_time_stamp, cost_time):
        insert_into_sql = "insert into ctl_fc_time(system_id,start_date,end_date,Data_Date,cost_time) values"
        insert_into_sql = "%s('%s',to_date('%s','yyyy/mm/dd HH24:MI:SS'),to_date('%s','yyyy/mm/dd HH24:MI:SS'),'%s','%s')" % (insert_into_sql, self.name, start_date_time_stamp, end_date_time_stamp, time_stamp, cost_time)

        conn = cx_Oracle.connect(self.oracle_DB)
        cur = conn.cursor()
        cur.execute(insert_into_sql)
        conn.commit()
        conn.close()


'''###一天前的时间（数据文件时间，将要跑批时间）'''
time_stamp = (datetime.datetime.now() + datetime.timedelta(-1))
time_stamp = time_stamp.strftime("%Y%m%d")


#外层自动跑批,由并发多进程（注意！！是多进程调用）
def auto_java_func(DB_name, DB_rename):
    #原本单个实例化，变成由并发多进程 同时实例化同时进行批处理动作
    # fchry_sql = oracle_run_sql_calss("fchry", 226, "select * from ctl_fc")
    # fchry_auto_java = auto_java_DB_class("fchry", fchry_sql)
    # fchry_auto_java.run_auto_java_DB_func()

    #编凑实例化，这里有两个类
    DB_sql = "%s_sql" % (DB_name)
    DB_auto_java = "%s_auto_java" % (DB_name)

    ###变量式实例化，shell爱用的峰哥
    DB_sql = oracle_run_sql_calss(DB_name, 226, "select * from ctl_fc")
    DB_auto_java = auto_java_DB_class(DB_name, DB_rename, DB_sql)

    #检查是否已经传来数据
    DB_auto_java.check_custom_upload_data_func()

    #检查是否已经跑过批
    DB_auto_java.check_DB_update_time_table_flag_func()

    #检查跑批是否正在运行
    DB_auto_java.check_auto_java_DB_isRunning_func()

    #真正跑批
    DB_auto_java.run_auto_java_DB_func()

if __name__ == '__main__':
    # conf_file = "/server/scripts/auto_java_DB_everyday/auto_java_scripts_sbs/DBlist.txt"
    #win
    conf_file = "DBlist.txt"

    #进程池，同时8个
    pool = multiprocessing.Pool(8)

    #读配置文件，按照数据库名进行进程池添加
    with open(conf_file, "r") as f:
        for line in f.readlines():
            line = line.strip()
            DB_name = line.split()[0]
            DB_rename = line.split()[1]
            pool.apply_async(auto_java_func, args=(DB_name, DB_rename))
    pool.close()
    pool.join()