import cx_Oracle, sys, datetime, time, subprocess, os, atexit, string, multiprocessing
from signal import SIGTERM

# oracle操作类
class oracle_run_sql_class(object):
    now_date = (datetime.datetime.now() + datetime.timedelta(-1)).strftime("%Y%m%d")

    def __init__(self, oracle_DB):
        self.oracle_DB = oracle_DB
        if oracle_DB == 223:
            self.oracle_DB = "etl/etl@10.138.22.223:1521/edw"
        elif oracle_DB == 226:
            self.oracle_DB = "etl/etl_Haier@10.138.22.226:1521/edw"

    # 查询标识表方法
    def search_sql_func(self, search_sql):
        try:
            conn = cx_Oracle.connect(self.oracle_DB)
            cur = conn.cursor()
            cur.execute(search_sql)
            result = cur.fetchall()
            cur.close()
            conn.close()
            return result
        except BaseException as e:
            print(e)
            print("Oracle search is broken!!!")

def foo(func):
    def _deco(arg1, arg2):
        start_time = time.time()
        ret = func(arg1, arg2)
        end_time = time.time()
        cost_time = end_time - start_time
        cost_time = round(cost_time / 60, 2)
        print(cost_time)
        return ret

    return _deco

class Call_StoredProcedure_Class(object):
    def __init__(self, Server_host):

        # 返回值必须是这样的，这里写成了全局变量
        global func_return_code, func_return_message
        func_return_code = ''
        func_return_message = '1234567890123456789012345'
        self.Server_host = Server_host
        if Server_host == 223:
            self.Server_host_id = 'dw/dw@10.138.22.223:1521/edw'
        elif Server_host == 226:
            self.Server_host_id = 'wsd/wsd@10.138.22.226:1521/edw'

    @foo
    def EveryDay_Task_Func(self, task_group_id, task_type, run_time):
        pass
        ##查询到对应存储过程名
        ##又得用到

        # 调取存储过程代码
    def Call_StoredProcedure(self, StoredProcedure_Name, data_date):
        print("正在调用 存储过程[%s] 日期[%s] ..." % (StoredProcedure_Name, data_date))
        # conn = cx_Oracle.connect(self.Server_host_id)
        # cur = conn.cursor()
        # res = cur.callproc(StoredProcedure_Name, [data_date, func_return_code, func_return_message])
        # print(res, "\n")
        # cur.close()
        # conn.close()

# 子程序1.1抽取 字典 配置表 1 找出所有作业组
def get_config_json():
    task_group_list = []
    search_sql = "select DISTINCT(group_id) from etl.t_Jobs_Order"
    task_obj = oracle_run_sql_class(223)
    result = task_obj.search_sql_func(search_sql)
    for i in result:
        task_group_list.append(i[0])

    task_group_dict = search_task_group_type_and_detail_func(task_group_list)
    return task_group_dict

##子程序1.2抽取 字典 配置表 ，根据 作业组 调用频率类型 及 对应精确时间
def search_task_group_type_and_detail_func(task_group_list):
    #把配置表信息 做出一个 字典， 这是一个配置字典
    task_group_dict = {}
    for task_group_id in task_group_list:
        task_group_dict[task_group_id] = {}
        search_sql = "select DISTINCT(duration) from etl.t_Jobs_Frequency where group_id='%s'" % (task_group_id)
        search_sql_obj = oracle_run_sql_class(223)
        result = search_sql_obj.search_sql_func(search_sql)

        for line2 in result:
            for line in range(len(line2)):
                kk = str(line2[line])
                task_group_dict[task_group_id][kk] = []

        # print(task_group_dict[task_group_id].keys())

        for task_type in task_group_dict[task_group_id].keys():
            #001在这里代表 任务类型为每天调用 ，这里找出 任务类型为每天调用 的具体时间
            #具体到 配置表 duration = 001 时 去查询 run_time 字段
            call_every_day_col = 'run_time'
            search_sql = "select %s from etl.t_Jobs_Frequency where group_id='%s' and duration = '%s'" % (call_every_day_col, task_group_id, task_type)
            search_sql_obj = oracle_run_sql_class(223)
            result = search_sql_obj.search_sql_func(search_sql)
            for line2 in result:
                for line in range(len(line2)):
                    kk = line2[line]
                    if kk:
                        task_group_dict[task_group_id][task_type].append(kk)

    return task_group_dict

##判断作业组是否应该执行（是否到点了）
def judge_the_task_group_run_func(task_group_dict):

    now_date_time = (datetime.datetime.now() + datetime.timedelta(-1)).strftime("%H:%S")
    now_date_time = "%2s" % (now_date_time)

    #循环作业组
    for task_group_id in task_group_dict.keys():
        #循环作业组作业类型
        for task_type in task_group_dict[task_group_id].keys():
            #循环作业组作业类型不为空
            if task_group_dict[task_group_id][task_type]:
                #作业组的run_time
                for run_time in task_group_dict[task_group_id][task_type]:

                    if run_time == "09:00":
                        print("请启动作业程序：", "作业组：", task_group_id, "作业类型：", task_type, "作业时间：", run_time)


#主程序
def main_func():
    # task_group_dict = get_config_json()
    # print(task_group_dict)
    task_group_dict = {1: {'001': ['07:30', '08:30'], '002': ['1', '3', '5'], '003': []}, 2: {'001': ['09:00', '07:00', '06:00'], '002': ['6', '7', '8']}}
    judge_the_task_group_run_func(task_group_dict)

#程序入口
main_func()

