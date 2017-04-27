import cx_Oracle, sys, datetime, time, subprocess, os, atexit, string, multiprocessing, copy
from signal import SIGTERM
import threading


# 守护进程包裹类
class Daemon(object):
    def __init__(self, pidfile, stdin='/dev/null', stdout='/dev/null', stderr='/dev/null'):
        # 需要获取调试信息，改为stdin='/dev/stdin', stdout='/dev/stdout', stderr='/dev/stderr'，以root身份运行。
        self.stdin = stdin
        self.stdout = stdout
        self.stderr = stderr
        self.pidfile = pidfile

    def _daemonize(self):
        try:
            pid = os.fork()  # 第一次fork，生成子进程，脱离父进程
            if pid > 0:
                sys.exit(0)  # 退出主进程
        except OSError as e:
            sys.stderr.write('fork #1 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)

        os.chdir("/")  # 修改工作目录
        os.setsid()  # 设置新的会话连接
        os.umask(0)  # 重新设置文件创建权限

        try:
            pid = os.fork()  # 第二次fork，禁止进程打开终端
            if pid > 0:
                sys.exit(0)
        except OSError as e:
            sys.stderr.write('fork #2 failed: %d (%s)\n' % (e.errno, e.strerror))
            sys.exit(1)

        # 重定向文件描述符
        sys.stdout.flush()
        sys.stderr.flush()

        # dup2函数原子化地关闭和复制文件描述符，重定向到/dev/nul，即丢弃所有输入输出
        si = open(self.stdin, 'r')
        so = open(self.stdout, 'a+')
        se = open(self.stderr, 'a+')
        os.dup2(si.fileno(), sys.stdin.fileno())
        os.dup2(so.fileno(), sys.stdout.fileno())
        os.dup2(se.fileno(), sys.stderr.fileno())

        # 注册退出函数，根据文件pid判断是否存在进程
        atexit.register(self.delpid)
        pid = str(os.getpid())
        open(self.pidfile, 'w+').write('%s\n' % pid)

    def delpid(self):
        os.remove(self.pidfile)

    def start(self):
        # 检查pid文件是否存在以探测是否存在进程
        try:
            pf = open(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if pid:
            message = 'pidfile %s already exist. Daemon already running!\n'
            sys.stderr.write(message % self.pidfile)
            sys.exit(1)

            # 启动监控
        self._daemonize()
        self._run()

    def stop(self):
        # 从pid文件中获取pid
        try:
            pf = open(self.pidfile, 'r')
            pid = int(pf.read().strip())
            pf.close()
        except IOError:
            pid = None

        if not pid:  # 重启不报错
            message = 'pidfile %s does not exist. Daemon not running!\n'
            sys.stderr.write(message % self.pidfile)
            return

            # 杀进程
        try:
            while 1:
                os.kill(pid, SIGTERM)
                time.sleep(0.1)
        except OSError as err:
            err = str(err)
            if err.find('No such process') > 0:
                if os.path.exists(self.pidfile):
                    os.remove(self.pidfile)
            else:
                print(str(err))
                sys.exit(1)

    def restart(self):
        self.stop()
        self.start()

    def _run(self):
        """ run your fun"""
        while True:
            main_func()
            time.sleep(1)

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

#抽取配置文件转换类
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

        #把结果暂时保存  减少查询sql
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
                        task_time_config_dict['task_group_id'][task_group_id]['call_type'][call_type]['run_time'].append(str(line[2]))
        #返回最后的json配置
        # print('last dict:', task_time_config_dict)
        return task_time_config_dict

    # 作业调度顺序表 抽出 json格式 t_Jobs_Order 详细的作业信息
    def get_task_group_info_dict(self):
        task_group_info_dict = {}
        task_group_info_dict['task_group_id'] = {}

        #把结果暂时保存  减少查询sql
        search_sql = "select group_id, JOB_ID, PREJOB_ID, OTHER_SELECT_SQL, PRO_NAME, PARA_IN, PARA_OUT from t_Jobs_Order"
        # search_sql = "select * from t_Jobs_Order"
        search_sql_obj = oracle_run_sql_class(self.which_db)
        result = search_sql_obj.search_sql_func(search_sql)
        task_group_info_list = copy.deepcopy(result)

        # 所有作业组
        for line in task_group_info_list:
            task_group_info_dict['task_group_id'][str(line[0])] = {}

        # 所有作业 及作业组的外部依赖
        for task_group_id in  task_group_info_dict['task_group_id'].keys():
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
                        task_group_info_dict['task_group_id'][task_group_id]['job_id'][job_id]['prejob_id'].append(str(line[2]))
                        task_group_info_dict['task_group_id'][task_group_id]['job_id'][job_id]['storeprodure_name'] = str(line[4])
                        task_group_info_dict['task_group_id'][task_group_id]['job_id'][job_id]['para_info'] = str(line[5])
                        task_group_info_dict['task_group_id'][task_group_id]['job_id'][job_id]['return_info'] = str(line[6])

        # print(task_group_info_dict)
        return task_group_info_dict

###查询时间多线程并发
def check_TaskGroup_RunTime_func(task_time_config_dict):
    #线程池
    thread_list = []
    for task_group_id in task_time_config_dict['task_group_id'].keys():
        t = threading.Thread(target=_check_TaskGroup_RunTime_func, args=(task_group_id, task_time_config_dict['task_group_id'][task_group_id],))
        t.start()
        thread_list.append(t)
    for t in thread_list:
        t.join()

#并发函数
def _check_TaskGroup_RunTime_func(task_group_id, call_type_info):
    status_file_name = 'status.txt'

    # 没有文件就创建文件
    if not os.path.isfile(status_file_name):
        with open(status_file_name, 'a') as f:
            pass

    #时间定义参数
    now_date = ((datetime.datetime.now()) + datetime.timedelta(days=-1)).strftime("%Y%m%d")
    now_date_time = (datetime.datetime.now()).strftime("%H:%M")
    now_date_time = '07:00'

    #查看当前时间 是否 对的上
    for call_type_id in call_type_info['call_type']:
        for run_time in call_type_info['call_type'][call_type_id]['run_time']:

            #如果时间对上了，看看是不是在调用了
            if now_date_time == run_time:
                # print(task_group_id, call_type_id, run_time)
                #done?
                now_done_statu = check_TaskGroup_RunStatusDone_func(task_group_id, run_time, now_date)

                #running?
                if not now_done_statu:
                    now_run_statu = check_TaskGroup_RunStatusRunning_func(task_group_id, run_time, now_date)

                ## will run..
                if not now_done_statu and not now_run_statu:
                    will_call_task_group_dict = {}
                    will_call_task_group_dict['task_group_id'] = str(task_group_id)
                    will_call_task_group_dict['call_type_id'] = str(call_type_id)
                    will_call_task_group_dict['now_date'] = str(now_date)
                    will_call_task_group_dict['run_time'] = str(run_time)


                    # wirte_running_flag
                    time.sleep(1)
                    with open(status_file_name, 'r+') as f:
                        wirte_running_flag = True
                        for line in f:
                            line = line.strip()
                            line_list = line.split(' ')
                            if line_list[0] == str(task_group_id) and line_list[1] == now_date and line_list[
                                2] == run_time and line_list[3] == 'R':
                                wirte_running_flag = False
                        if wirte_running_flag:
                            print(task_group_id, now_date, run_time, 'R', '写入标识')
                            print(task_group_id, now_date, run_time, 'R', file=f)

                    ##调用另一个跑批脚本
                    # print('call SP running .....', will_call_task_group_dict)
                    Run_file = "%s/Auto_Call_SP_Run.py" % (os.path.dirname(__file__))
                    cmd = "python %s %s %s %s %s" % (Run_file, task_group_id, call_type_id, now_date, run_time)
                    subprocess.run(cmd, check=True)


##检测调度运行完毕
def check_TaskGroup_RunStatusDone_func(task_group_id, run_time, now_date):
    status_file_name = 'status.txt'
    now_done_statu = False
    ##done?
    with open(status_file_name, 'r') as f:
        for line in f:
            line = line.strip()
            line_list = line.split(' ')
            if line_list[0] == str(task_group_id) and line_list[1] == now_date and line_list[2] == run_time and \
                            line_list[3] == 'D':
                print('今天任务已经完成!', task_group_id,now_date, run_time, 'D')
                now_done_statu = True
                return now_done_statu
    return now_done_statu

##检测调度运行zhengzai运行
def check_TaskGroup_RunStatusRunning_func(task_group_id, run_time, now_date):
    status_file_name = 'status.txt'

    now_run_statu = False
    ##done?
    with open(status_file_name, 'r') as f:
        for line in f:
            line = line.strip()
            line_list = line.split(' ')
            if line_list[0] == str(task_group_id) and line_list[1] == now_date and line_list[
                2] == run_time and \
                            line_list[3] == 'R':
                print('今天任务正在运行...', task_group_id, now_date, run_time, 'R')
                now_run_statu = True
                return now_run_statu

    return now_run_statu


#主程序
def main_func():
    #取配置文件
    db226_config = Get_Config_Json_Class(226)
    task_time_config_dict = db226_config.get_task_time_config_func()

    check_TaskGroup_RunTime_func(task_time_config_dict)

if __name__ == '__main__':
    main_func()

# if __name__ == '__main__':
#     status_file_name = 'status.txt'
#     daemon = Daemon('/var/run/call_SP1.pid', stdout='/var/log/call_SP_stdout1.log', stderr="/var/log/call_SP_stderr1.log")
#     if len(sys.argv) == 2:
#         if 'start' == sys.argv[1]:
#             daemon.start()
#         elif 'stop' == sys.argv[1]:
#             daemon.stop()
#         elif 'restart' == sys.argv[1]:
#             daemon.restart()
#         else:
#             print('unknown command')
#             sys.exit(2)
#         sys.exit(0)
#     else:
#         print('usage: %s start|stop|restart' % sys.argv[0])
#         sys.exit(2)