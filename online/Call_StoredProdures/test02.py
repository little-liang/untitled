import cx_Oracle, sys, datetime, time, subprocess, os, atexit, string, multiprocessing
from signal import SIGTERM

# oracle操作类
class oracle_run_sql_class(object):
    def __init__(self, oracle_DB):
        self.oracle_DB = oracle_DB
        if oracle_DB == 223:
            self.oracle_DB = "etl/etl@10.138.22.223:1521/edw"
        elif oracle_DB == 226:
            self.oracle_DB = "etl/etl_Haier@10.138.22.226:1521/edw"
        self.now_date = (datetime.datetime.now() + datetime.timedelta(-1)).strftime("%Y%m%d")

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

#主函数 //这里查时间
def main_func(task_group_id):
    print(task_group_id)
    search_sql = "select * from etl.t_Jobs_Frequency"
    search_sql_obj = oracle_run_sql_class(223)
    result = search_sql_obj.search_sql_func(search_sql)
    print(result)

#多进程并发包裹
def main_MultiPro_func(task_group_list):
    # 进程池，同时8个
    pool = multiprocessing.Pool(len(task_group_list))
    # 读配置文件，按照数据库名进行进程池添加
    for task_group_id in task_group_list:
        pool.apply_async(main_func, args=(str(task_group_id)))
    pool.close()
    pool.join()


if __name__ == '__main__':
    task_group_list = []
    #判断起来多少个进程
    search_sql = "select DISTINCT(group_id) from etl.t_Jobs_Order"
    task_obj = oracle_run_sql_class(223)
    result = task_obj.search_sql_func(search_sql)
    for i in result:
        task_group_list.append(i[0])
    main_MultiPro_func(task_group_list)
