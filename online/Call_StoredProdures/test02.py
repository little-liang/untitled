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

# 子程序1.1抽取 字典 配置表 1 找出所有作业组
def get_config_json():
    task_group_list = []
    search_sql = "select DISTINCT(group_id) from etl.t_Jobs_Order"
    task_obj = oracle_run_sql_class(223)
    result = task_obj.search_sql_func(search_sql)
    for i in result:
        task_group_list.append(i[0])

    # print(task_group_list)
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
            call_every_day_code = '001'
            call_every_day_col = 'run_time'
            if task_type == call_every_day_code:
                search_sql = "select %s from etl.t_Jobs_Frequency where group_id='%s' and duration = '%s'" % (call_every_day_col, task_group_id, call_every_day_code)
                search_sql_obj = oracle_run_sql_class(223)
                result = search_sql_obj.search_sql_func(search_sql)
                for line2 in result:
                    for line in range(len(line2)):
                        kk = str(line2[line])
                        task_group_dict[task_group_id][call_every_day_code].append(kk)

        #002在这里代表 任务类型为每周调用 ，这里找出 任务类型为每周调用 的具体天
        #具体到 配置表 duration = 002 时 去查询 run_date 字段
        call_every_day_code = '002'
        call_every_day_col = 'run_date'
        if task_type == call_every_day_code:
            search_sql = "select %s from etl.t_Jobs_Frequency where group_id='%s' and duration = '%s'" % (call_every_day_col, task_group_id, call_every_day_code)
            search_sql_obj = oracle_run_sql_class(223)
            result = search_sql_obj.search_sql_func(search_sql)
            for line2 in result:
                for line in range(len(line2)):
                    kk = str(line2[line])
                    task_group_dict[task_group_id][call_every_day_code].append(kk)

    # print(task_group_dict)
    return task_group_dict


#主程序
def main_func():
    task_group_dict = get_config_json()
    print(task_group_dict)

#程序入口
main_func()

# #多进程并发包裹
# def main_MultiPro_func(task_group_list):
#     # 进程池，同时8个
#     pool = multiprocessing.Pool(len(task_group_list))
#     # 读配置文件，按照数据库名进行进程池添加
#     for task_group_id in task_group_list:
#         pool.apply_async(main_func, args=(str(task_group_id)))
#     pool.close()
#     pool.join()


# if __name__ == '__main__':
#     task_group_list = []
#     #判断起来多少个进程
#     search_sql = "select DISTINCT(group_id) from etl.t_Jobs_Order"
#     task_obj = oracle_run_sql_class(223)
#     result = task_obj.search_sql_func(search_sql)
#     for i in result:
#         task_group_list.append(i[0])
#     main_MultiPro_func(task_group_list)
