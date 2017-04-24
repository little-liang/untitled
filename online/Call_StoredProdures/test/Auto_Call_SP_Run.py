import cx_Oracle, sys, datetime, time, subprocess, os, atexit, string, multiprocessing, copy
from signal import SIGTERM
import threading


# oracle操作类
class oracle_run_sql_class(object):
    db223_connect_info = "etc/etc@10.138.22.223:1521/edw"
    db226_connect_info = "etc/etc_Control@10.138.22.226:1521/edw"

    def __init__(self, which_db):
        self.which_db = which_db
        if which_db == 223:
            self.which_db = oracle_run_sql_class.db223_connect_info
        elif which_db == 226:
            self.which_db = oracle_run_sql_class.db226_connect_info

    @staticmethod
    def get_user_passwd(which_db):
        if which_db == 223:
            self.which_db = oracle_run_sql_class.db223_connect_info
        elif which_db == 226:
            self.which_db = oracle_run_sql_class.db226_connect_info
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

    # 通用版本
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

    # 调取存储过程代码
    def Call_StoredProcedure(self, StoredProcedure_Name, para_list):
        conn = cx_Oracle.connect(self.which_db)
        cur = conn.cursor()
        res = cur.callproc(StoredProcedure_Name, para_list)
        print(res)
        cur.close()
        conn.close()
        return res


# 抽取配置文件转换类
class Get_Config_Json_Class(object):
    def __init__(self, which_db):
        self.which_db = which_db
        if which_db == 223:
            self.which_db = oracle_run_sql_class.db223_connect_info
        elif which_db == 226:
            self.which_db = oracle_run_sql_class.db226_connect_info

    ##作业调度时刻表 抽出 json格式 t_Jobs_Frequency
    def get_task_time_config_func(self):
        task_time_config_dict = {}
        task_time_config_dict['task_group_id'] = {}

        # 把结果暂时保存  减少查询sql
        search_sql = "select group_id, duration, run_time from t_Jobs_frequency"
        search_sql_obj = oracle_run_sql_class(self.which_db)
        result = search_sql_obj.search_sql_func(search_sql)
        run_time_config_list = copy.deepcopy(result)
        # print(run_time_config_list)

        # 所有作业组
        for line in run_time_config_list:
            task_time_config_dict['task_group_id'][str(line[0])] = {}

        # 所有的调度类型
        for task_group_id in task_time_config_dict['task_group_id'].keys():
            task_time_config_dict['task_group_id'][task_group_id]['call_type'] = {}
            for line in run_time_config_list:
                if str(line[0]) == task_group_id:
                    task_time_config_dict['task_group_id'][task_group_id]['call_type'][str(line[1])] = {}

        # 所有的调度类型后的时间
        for task_group_id in task_time_config_dict['task_group_id'].keys():
            for call_type in task_time_config_dict['task_group_id'][task_group_id]['call_type'].keys():
                task_time_config_dict['task_group_id'][task_group_id]['call_type'][call_type]['run_time'] = []
                for line in run_time_config_list:
                    if str(line[0]) == task_group_id and str(line[1]) == call_type:
                        task_time_config_dict['task_group_id'][task_group_id]['call_type'][call_type][
                            'run_time'].append(str(line[2]))
        # 返回最后的json配置
        # print('last dict:', task_time_config_dict)
        return task_time_config_dict

    # 作业调度顺序表 抽出 json格式 t_Jobs_Order 详细的作业信息
    def get_task_group_info_dict(self):
        task_group_info_dict = {}
        task_group_info_dict['task_group_id'] = {}

        # 把结果暂时保存  减少查询sql
        search_sql = "select group_id, JOB_ID, PREJOB_ID, OTHER_SELECT_SQL, PRO_NAME, PARA_IN, PARA_OUT from t_Jobs_Order"
        # search_sql = "select * from t_Jobs_Order"
        search_sql_obj = oracle_run_sql_class(self.which_db)
        result = search_sql_obj.search_sql_func(search_sql)
        task_group_info_list = copy.deepcopy(result)

        # 所有作业组
        for line in task_group_info_list:
            task_group_info_dict['task_group_id'][str(line[0])] = {}

        # 所有作业 及作业组的外部依赖
        for task_group_id in task_group_info_dict['task_group_id'].keys():
            task_group_info_dict['task_group_id'][task_group_id]['job_id'] = {}
            task_group_info_dict['task_group_id'][task_group_id]['other_prejob'] = ''
            for line in task_group_info_list:
                if str(line[0]) == task_group_id:
                    task_group_info_dict['task_group_id'][task_group_id]['job_id'][str(line[1])] = {}
                    task_group_info_dict['task_group_id'][task_group_id]['other_prejob'] = str(line[3])

        # 所有作业的 pre_job_id, PRO_NAME, PARA_IN, PARA_OUT
        for task_group_id in task_group_info_dict['task_group_id'].keys():
            for job_id in task_group_info_dict['task_group_id'][task_group_id]['job_id'].keys():
                task_group_info_dict['task_group_id'][task_group_id]['job_id'][job_id]['prejob_id'] = []
                task_group_info_dict['task_group_id'][task_group_id]['job_id'][job_id]['storeprodure_name'] = ''
                task_group_info_dict['task_group_id'][task_group_id]['job_id'][job_id]['para_info'] = ''
                task_group_info_dict['task_group_id'][task_group_id]['job_id'][job_id]['return_info'] = ''

                for line in task_group_info_list:
                    if str(line[0]) == task_group_id and job_id == str(line[1]):
                        task_group_info_dict['task_group_id'][task_group_id]['job_id'][job_id]['prejob_id'].append(
                            str(line[2]))
                        task_group_info_dict['task_group_id'][task_group_id]['job_id'][job_id][
                            'storeprodure_name'] = str(line[4])
                        task_group_info_dict['task_group_id'][task_group_id]['job_id'][job_id]['para_info'] = str(
                            line[5])
                        task_group_info_dict['task_group_id'][task_group_id]['job_id'][job_id]['return_info'] = str(
                            line[6])

        # print(task_group_info_dict)
        return task_group_info_dict


# 检查call_type的类型接口
def check_task_call_type(will_call_task_group_dict):
    db223_config = Get_Config_Json_Class(223)
    task_group_info_dict = db223_config.get_task_group_info_dict()
    task_group_info_dict['task_group_id'][will_call_task_group_dict['task_group_id']]['data_date'] = \
        will_call_task_group_dict['now_date']

    task_group_info_dict['task_group_id'][will_call_task_group_dict['task_group_id']]['run_time'] = \
        will_call_task_group_dict['run_time']

    task_group_info_dict['task_group_id'][will_call_task_group_dict['task_group_id']]['call_type_id'] = \
        will_call_task_group_dict['call_type_id']

    for job_id in task_group_info_dict['task_group_id'][will_call_task_group_dict['task_group_id']]['job_id'].keys():
        all_done_SP_set.add(job_id)

    return task_group_info_dict


##调度
def call_SP(task_group_id, task_group_info_dict):
    ##类型1
    if task_group_info_dict['call_type_id'] == '1':
        _call_SP_1(task_group_id, task_group_info_dict)


def _call_SP_1(task_group_id, task_group_info_dict):
    # 1外部依赖
    search_sql_obj = oracle_run_sql_class(223)
    search_sql = "select other_select_sql from t_Jobs_order where group_id = '%s' and job_id = 1" % (task_group_id)
    res = search_sql_obj.search_sql_func(search_sql)

    pre_other_job_info = res[0][0]
    if pre_other_job_info == None:
        print("作业组 %s 无 外部依赖" % (task_group_id))
        pass
    else:
        search_sql_obj = oracle_run_sql_class(223)
        res = search_sql_obj.search_sql_func(pre_other_job_info)
        print('外部依赖作业情况', res[0][0])
        pre_other_job_flag = res[0][0]

        if pre_other_job_flag == 1:
            print("外部依赖作业未完成,exit!")
            sys.exit(0)

    # 优先级判断外部做不了现在直接并发，等待线程完成
    for job_id in task_group_info_dict['job_id'].keys():
        # 整理调用参数，最好写个接口
        call_SP_para_list = []
        # 参数 存储过程名
        storeprodure_name = task_group_info_dict['job_id'][job_id]['storeprodure_name']
        para_info_list = task_group_info_dict['job_id'][job_id]['para_info'].split(',')

        # 调用参数 固定昨天
        for line in para_info_list:
            if line == 'i_data_date':
                call_SP_para_list.append(task_group_info_dict['data_date'])

        # 调用参数，固定返回参数
        return_info = task_group_info_dict['job_id'][job_id]['return_info']
        return_info_list = task_group_info_dict['job_id'][job_id]['return_info'].split(',')

        for line in return_info_list:
            if line == 'o_return_code':
                # 默认填充参数
                call_SP_para_list.append('0000000000000000000000000000000000000000000000')
            elif line == 'o_return_message':
                call_SP_para_list.append('0000000000000000000000000000000000000000000000')

        # 得到最后调用参数
        # print(call_SP_para_list, 'gffffffffff')

        # 多线程并发
        thread_list = []

        t = threading.Thread(target=call_SP_run, args=(
            task_group_id, job_id, task_group_info_dict['job_id'][job_id]['prejob_id'], storeprodure_name,
            call_SP_para_list, task_group_info_dict))
        t.start()
        thread_list.append(t)

        # 等待线程运行完毕
    for t in thread_list:
        t.join()


# 内部依赖 加 调用
def call_SP_run(task_group_id, job_id, prejob_id, storeprodure_name, call_SP_para_list, task_group_info_dict):
    status_file_name = 'status.txt'
    start_time = (datetime.datetime.now()).strftime("%Y%m%d %H:%M:%S")
    while True:
        if str(prejob_id[0]) in done_SP_set:
            call_SP_obj = Call_StoredProcedure_Class(223)
            call_result = call_SP_obj.Call_StoredProcedure(storeprodure_name, call_SP_para_list)
            print("作业组 [%s], 作业[%s], 已完成" % (task_group_id, job_id))

            # 单个job跑完就写入日志表
            data_date = task_group_info_dict['data_date']
            wirte_table_log(data_date, task_group_id, job_id, storeprodure_name, start_time,
                            task_group_info_dict['run_time'])

            # 全部完成,状态
            lock.acquire()
            done_SP_set.add(job_id)
            if done_SP_set == all_done_SP_set:
                print("作业组 [%s] 全部作业已经完成！" % (task_group_id))
                # 写入状态文件
                wirte_running_flag(task_group_id, task_group_info_dict['data_date'], task_group_info_dict['run_time'])
            lock.release()

            break
        else:
            time.sleep(1)


def wirte_running_flag(task_group_id, data_date, run_time):
    status_file_name = 'status.txt'
    # wirte_running_flag
    with open(status_file_name, 'r+') as f:
        wirte_running_flag = True
        for line in f:
            line = line.strip()
            line_list = line.split(' ')
            if line_list[0] == str(task_group_id) and line_list[1] == data_date and line_list[
                2] == run_time and line_list[3] == 'D':
                wirte_running_flag = False
                print(task_group_id, data_date, run_time, 'D', "状态(status.txt)标志已经更新")
        if wirte_running_flag:
            print(task_group_id, data_date, run_time, 'D', file=f)
            print(task_group_id, data_date, run_time, 'D', "状态(status.txt)标志已经更新")

def wirte_table_log(data_date, task_group_id, job_id, storeprodure_name, start_time, run_time):
    run_status = 'D'
    end_time = (datetime.datetime.now()).strftime("%Y%m%d %H:%M:%S")
    insert_sql = "insert into t_jobs_logs(DATA_DAY,GROUP_ID,JOB_ID,PRO_NAME,RUN_START_TIME,RUN_END_TIME,RUN_STATUS,RUN_TIME) values('%s', %d, %d, '%s', to_date('%s','yyyymmdd hh24:mi:SS'), to_date('%s','yyyymmdd hh24:mi:SS'),'%s','%s')" % \
                 (
                     data_date, int(task_group_id), int(job_id), storeprodure_name, start_time, end_time, run_status, run_time
                 )
    insert_sql_obj = oracle_run_sql_class(223)
    insert_sql_obj.insert_into_sql_common_func(insert_sql)

    print("作业组[%s] 作业[%s] 日期[%s] 时间[%s] 已经写入日志表" % (task_group_id, job_id, data_date, run_time))


# 主程序
def main_func(will_call_task_group_dict):
    global done_SP_set
    done_SP_set = set()
    done_SP_set.add('None')

    global all_done_SP_set
    all_done_SP_set = set()
    all_done_SP_set.add('None')

    global lock
    lock = threading.Lock()

    # 传入参数模拟
    # will_call_task_group_dict = {'call_type_id': '1', 'run_time': '07:00', 'now_date': '20170424', 'task_group_id': '1'}

    # 检查调度类型
    task_group_info_dict = check_task_call_type(will_call_task_group_dict)

    ##调用存储过程
    call_SP(will_call_task_group_dict['task_group_id'],
            task_group_info_dict['task_group_id'][will_call_task_group_dict['task_group_id']])


if __name__ == '__main__':
    # 1 20170421 07:00传递参数是 可能 超过1天 23:00 跑到 次日1:00
    # 参数为 task_group_id, call_type_id, data_date, run_time

    will_call_task_group_dict = {}
    will_call_task_group_dict['task_group_id'] = sys.argv[1]
    will_call_task_group_dict['call_type_id'] = sys.argv[2]
    will_call_task_group_dict['now_date'] = sys.argv[3]
    will_call_task_group_dict['run_time'] = sys.argv[4]

    print(will_call_task_group_dict)
    print("作业组[%s] 日期 [%s] 时间 [%s] 正在调用" % (
    will_call_task_group_dict['task_group_id'], will_call_task_group_dict['now_date'], will_call_task_group_dict))
    main_func(will_call_task_group_dict)
