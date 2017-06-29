import cx_Oracle, sys, datetime, time, subprocess, os, atexit, string, multiprocessing, copy
from signal import SIGTERM
import threading

# oracle操作类
class oracle_run_sql_class(object):
    db223_connect_info = "etc/etc@10.138.22.223:1521/edw"
    db226_connect_info = "etc/etc_Control@10.138.22.226:1521/edw"
    db226dw_connect_info = "dw/Haier_dw@10.138.22.226:1521/edw"


    def __init__(self, which_db):
        self.which_db = which_db
        if which_db == 223:
            self.which_db = oracle_run_sql_class.db223_connect_info
        elif which_db == 226:
            self.which_db = oracle_run_sql_class.db226_connect_info
        elif which_db == 'db226dw':
            self.which_db = oracle_run_sql_class.db226dw_connect_info


    @staticmethod
    def get_user_passwd(which_db):
        if which_db == 223:
            self.which_db = oracle_run_sql_class.db223_connect_info
        elif which_db == 226:
            self.which_db = oracle_run_sql_class.db226_connect_info
        elif which_db == 'db226dw':
            self.which_db = oracle_run_sql_class.db226dw_connect_info
        return self.which_db

    # 查询标识表方法
    def search_sql_func(self, search_sql):
        try:
            conn = cx_Oracle.connect(self.which_db)
            cur = conn.cursor()
            cur.execute(search_sql)
            result = cur.fetchall()
            cur.close()
            conn.close()
            return result
        except BaseException as e:
            print(e)
            print("Oracle connect is broken!!!")

    #通用版本
    def insert_into_sql_common_func(self, insert_sql):
        try:
            conn = cx_Oracle.connect(self.which_db)
            cur = conn.cursor()
            cur.execute(insert_sql)
            conn.commit()
            conn.close()
        except BaseException as e:
            print(e)
            print("Oracle connect is broken!!!")

# 调用存储过程类
class Call_StoredProcedure_Class(object):
    def __init__(self, which_db):
        self.which_db = which_db
        if which_db == 223:
            self.which_db = oracle_run_sql_class.db223_connect_info
        elif which_db == 226:
            self.which_db = oracle_run_sql_class.db226_connect_info
        elif which_db == 'db226dw':
            self.which_db = oracle_run_sql_class.db226dw_connect_info

    # 调取存储过程代码
    def Call_StoredProcedure(self, StoredProcedure_Name, para_list):
        print(StoredProcedure_Name, "start")
        conn = cx_Oracle.connect(self.which_db)
        cur = conn.cursor()
        res = cur.callproc(StoredProcedure_Name, para_list)
        cur.close()
        conn.close()
        print(StoredProcedure_Name, "ok")
        return res


##检查是否任务正在运行
def check_command_submit(now_date):
    now_date_time = (datetime.datetime.now()).strftime("[INFO] [%Y%m%d %H:%S]")

    if os.path.getsize(status_file_name) == 0:
        return True
    with open(status_file_name, 'r') as f:
        for line in f.readlines():
            line = line.strip()
            line_list = line.split(" ")
            if now_date == str(line_list[1]):
                print(now_date_time, now_date, 'is running, will exit!')
                exit()

##检查是否任务已经运行完毕
def check_run_done(now_date):
    search_sql_obj = oracle_run_sql_class(226)
    search_sql = "select DATA_DATE, RUN_STATUS from EDWPDC.EDWPDC_FINISH_FLAG where DATA_DATE = '%s' and RUN_STATUS ='0'" % (now_date)
    res = search_sql_obj.search_sql_func(search_sql)
    if res == []:
        return True
    if res[0][0] == now_date and res[0][1] == '0':
        print(now_date_time, now_date, '已经跑完了')
        exit()

##检查依赖
def check_pre_job_and_call_SP(SP_name, SP_name2):
    while True:
        search_sql_obj = oracle_run_sql_class('db226dw')
        search_sql = "select * from dw.dw_dependences WHERE data_date = '%s' and source_base = '%s' and run_status = '0'" % (now_date, SP_name2)
        # search_sql = "select * from dw.dw_dependences WHERE data_date = '20170101' and source_base = '%s' and run_status = '0'" % (SP_name2)
        res = search_sql_obj.search_sql_func(search_sql)

        if not res or res == []:
            print(SP_name, "依赖未完成")
            time.sleep(2)
            continue
        else:
            call_SP(SP_name)
            break

def write_run_flag():
    status = 'R'
    now_date_time = (datetime.datetime.now()).strftime("[%Y-%m-%d-%H:%S]")
    with open(status_file_name, "a+") as f:
        print(now_date_time, now_date, status, file=f)


def call_SP(SP_name):
    para_list = []
    para_list.append(date_yesterday)
    para_list.append('pppppppppppppppppppppppppppppppppppppppppppppppppp')
    para_list.append('pppppppppppppppppppppppppppppppppppppppppppppppppp')
    call_SP_obj = Call_StoredProcedure_Class(226)
    call_SP_obj.Call_StoredProcedure(SP_name, para_list)

def flush_run_flag():
    with open(status_file_name, "w") as f:
        pass


def delete_sql_func():
    now_date_time = (datetime.datetime.now()).strftime("%Y%m%d %H:%M:%S")
    insert_sql_obj = oracle_run_sql_class(226)
    insert_sql ="delete from EDWPDC.EDWPDC_FINISH_FLAG where DATA_DATE = '%s'" % (now_date)
    try:
        insert_sql_obj.insert_into_sql_common_func(insert_sql)
    except Exception as e:
        print(e)


# 插入完成标识
def insert_into_sql_func():
    now_date_time = (datetime.datetime.now()).strftime("%Y%m%d %H:%M:%S")
    insert_sql_obj = oracle_run_sql_class(226)
    insert_sql = "insert into EDWPDC.EDWPDC_FINISH_FLAG values('%s','0',to_date('%s','yyyymmdd hh24:mi:SS'))" % (
    now_date, now_date_time)
    try:
        insert_sql_obj.insert_into_sql_common_func(insert_sql)
    except Exception as e:
        print(e)

def multi_call_no_pre_job(pre_job_list):
    write_run_flag()
    # 定义线程池
    thread_list = []

    # 主进程开始
    for SP_name in pre_job_list:
        SP_name_list = SP_name.split("_")
        SP_name2 = SP_name_list[-1]

        t = threading.Thread(target=check_pre_job_and_call_SP, args=(SP_name, SP_name2))
        t.start()
        thread_list.append(t)

    ##等待子进程或者线程结束后再继续往下运行（这时候只要线程 池中有线程，就接着等。。）
    for t in thread_list:
        t.join()




if __name__ == '__main__':
    status_file_name = 'status.txt'
    now_date = (datetime.datetime.now()).strftime("%Y%m%d")
    now_date_time = (datetime.datetime.now()).strftime("[%Y%m%d %H:%M]")

    date_yesterday = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
    # # done?
    # check_run_done(now_date)
    #
    # # #runing?
    # check_command_submit(now_date)

    pre_job_list = (
        'dw.pack_dw_all_new_ETL.pack_dw_all_new_FT',
    )

    multi_call_no_pre_job(pre_job_list)

