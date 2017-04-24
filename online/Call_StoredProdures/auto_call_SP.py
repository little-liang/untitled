import cx_Oracle, sys, datetime, time, subprocess, os, atexit, string, multiprocessing
from signal import SIGTERM
import threading

'''最后描述出的字典格式！
aa = \
#     {
#         'task_group_id':
#             {
#                 1:
#                     {
#                         'job_id':
#                             {
#                                 1:
#                                     {
#                                         'para_info': 'i_data_date,i_type',
#                                         'run_time': '07:00',
#                                         'storeprodure_name': 'proc_all',
#                                     },
#                                 2:
#                                     {'para_info': 'i_data_date,i_type',
#                                      'run_time': '07:00',
#                                      'storeprodure_name': 'proc_all'
#                                      },
#                                 3:
#                                     {'para_info': 'i_data_date,i_type',
#                                      'run_time': '07:00',
#                                      'storeprodure_name': 'proc_all'
#                                      },
#                                 4:
#                                     {'para_info': 'i_data_date,i_type',
#                                      'run_time': '07:00',
#                                      'storeprodure_name': 'proc_all'
#                                      }
#                             }
#                     },
#
#                 2:
#                     {
#                         'job_id':
#                             {
#                                 1:
#                                     {
#                                         'para_info': 'date',
#                                         'run_time': '07:00',
#                                         'storeprodure_name': 'pro1',
#                                     },
#
#                                 2:
#                                     {
#                                         'para_info': 'date',
#                                         'run_time': '07:00',
#                                         'storeprodure_name': 'pro2',
#                                     }
#                             }
#                     }
#             }
#     }
#
# print(aa)

'''


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
    now_date = (datetime.datetime.now() + datetime.timedelta(-1)).strftime("%Y%m%d")

    def __init__(self, oracle_DB):
        self.oracle_DB = oracle_DB
        if oracle_DB == 223:
            self.oracle_DB = "etc/etc@10.138.22.223:1521/edw"
        # elif oracle_DB == 226:
        #     self.oracle_DB = "etl/etl_ff@10.138.22.226:1521/edw"

    def get_user_passwd(self, oracle_DB):
        if oracle_DB == 223:
            self.oracle_DB = "etc/etc@10.138.22.223:1521/edw"
        # elif oracle_DB == 226:
        #     self.oracle_DB = "etl/etl_ffr@10.138.22.226:1521/edw"
        return self.oracle_DB

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

    # 插入更新表
    def insert_into_sql_func(self, run_time, run_status):
        insert_into_sql = "insert into t_jobs_logs(run_time, run_status) values('%s', '%s')" % (run_time, run_status)
        conn = cx_Oracle.connect(self.oracle_DB)
        cur = conn.cursor()
        cur.execute(insert_into_sql)
        conn.commit()
        conn.close()

    #通用版本
    def insert_into_sql_common_func(self, insert_into_sql):
        conn = cx_Oracle.connect(self.oracle_DB)
        cur = conn.cursor()
        cur.execute(insert_into_sql)
        conn.commit()
        conn.close()

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
        self.Server_host = Server_host
        if Server_host == 223:
            self.Server_host_id = oracle_run_sql_class.get_user_passwd(oracle_run_sql_class, 223)
        elif Server_host == 226:
            self.Server_host_id = oracle_run_sql_class.get_user_passwd(oracle_run_sql_class, 223)

    @foo
    def Before_Call_StoredProcedure(self, StoredProcedure_Name_list):
        pass

        # 调取存储过程代码
    def Call_StoredProcedure(self, StoredProcedure_Name, para_list):
        conn = cx_Oracle.connect(self.Server_host_id)
        cur = conn.cursor()
        res = cur.callproc(StoredProcedure_Name, para_list)
        cur.close()
        conn.close()
        return res

# 子程序1.1抽取 字典 配置表 1 找出所有作业组
def get_config_json():
    task_group_list = []
    search_sql = "select DISTINCT(group_id) from t_Jobs_frequency"
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
        search_sql = "select DISTINCT(duration) from t_Jobs_Frequency where group_id='%s'" % (task_group_id)
        search_sql_obj = oracle_run_sql_class(223)
        result = search_sql_obj.search_sql_func(search_sql)

        for line2 in result:
            for line in range(len(line2)):
                kk = str(line2[line])
                task_group_dict[task_group_id][kk] = []

        for task_type in task_group_dict[task_group_id].keys():
            #001在这里代表 任务类型为每天调用 ，这里找出 任务类型为每天调用 的具体时间
            #具体到 配置表 duration = 001 时 去查询 run_time 字段
            call_every_day_col = 'run_time'
            search_sql = "select %s from t_Jobs_Frequency where group_id='%s' and duration = '%s'" % (call_every_day_col, task_group_id, task_type)
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
    #甩出来一个即将执行的任务字典
    will_start_task_group_flag = False
    will_start_task_group_dict = {}
    will_start_task_group_dict['task_group_id'] = {}

    # print(task_group_dict)
    now_date_time = (datetime.datetime.now()).strftime("%H:%S")
    now_date_time = "%2s" % (now_date_time)


    #循环作业组
    for task_group_id in task_group_dict.keys():
        will_start_task_group_dict['task_group_id'][task_group_id] = {}
        will_start_task_group_dict['task_group_id'][task_group_id]['job_id'] = {}
        #循环作业组作业类型
        for task_type in task_group_dict[task_group_id].keys():
            #循环作业组作业类型不为空
            if task_group_dict[task_group_id][task_type]:
                #作业组的run_time
                for run_time in task_group_dict[task_group_id][task_type]:

                    #当前时间,如果到时间了
                    # print(now_date_time)
                    now_date_time = '09:00'
                    if now_date_time == run_time:

                        will_start_task_group_flag = True
                        #查询出对应的作业组对应的job_id
                        search_sql = "select job_id from t_Jobs_order where group_id = '%s'" % (task_group_id)
                        task_obj = oracle_run_sql_class(223)
                        result = task_obj.search_sql_func(search_sql)
                        for job_id in result:
                            will_start_task_group_dict['task_group_id'][task_group_id]['job_id'][job_id[0]] = {}

                            # 查询出对应的作业组对应的job_id下的存储过程名字及参数
                            search_sql = "select pro_name, para_in, prejob_id from t_Jobs_order where group_id = '%s' and job_id = '%s'" % (task_group_id, job_id[0])
                            task_obj = oracle_run_sql_class(223)
                            result = task_obj.search_sql_func(search_sql)
                            for i in result:
                                will_start_task_group_dict['task_group_id'][task_group_id]['job_id'][job_id[0]]['storeprodure_name'] = i[0]
                                will_start_task_group_dict['task_group_id'][task_group_id]['job_id'][job_id[0]]['para_info'] = i[1]
                                will_start_task_group_dict['task_group_id'][task_group_id]['job_id'][job_id[0]]['prejob_id'] = i[2]
                                will_start_task_group_dict['task_group_id'][task_group_id]['job_id'][job_id[0]]['run_time'] = run_time

    # 如果可以调用的化
    if will_start_task_group_flag:
        now_date = (datetime.datetime.now()).strftime("%Y%m%d")
        #运行状态
        running_flag = True
        if not os.path.isfile(status_file_name):
            f = open(status_file_name, 'a')
            f.close()

        ##done!!!?
        f = open(status_file_name, 'r')
        file_list = f.readlines()
        for line in file_list:
            line = line.strip()
            line_list = line.split(' ')

            if line_list[0] == now_date and now_date_time == line_list[1] and line_list[2] == 'D':
                print(now_date, now_date_time, 'running done!!!, will exit!')
                running_flag = False
                break
        f.close()

        #running?
        if running_flag:
            f = open(status_file_name, 'r')
            file_list = f.readlines()
            for line in file_list:
                line = line.strip()
                line_list = line.split(' ')
                if line_list[0] == now_date and now_date_time == line_list[1] and line_list[2] == 'R':
                    print(now_date, now_date_time, 'running, will exit!')
                    running_flag = False
                    break
            f.close()

        if running_flag:
            print(now_date, now_date_time, "run......")
            f = open(status_file_name, 'a')
            print(now_date, now_date_time, 'R', file=f)

            print(now_date, now_date_time, "R", '已经写入')
            f.close()
            start_the_task_group_run_func(will_start_task_group_dict)
    else:
        print("没到指定的时间", now_date_time)

def empty_status_file_func():
    f = open(status_file_name, 'w')
    f.close()

##作业组执行程序 2.1 开始多进程
def start_the_task_group_run_func(will_start_task_group_dict):

    now_date = (datetime.datetime.now()).strftime("%Y%m%d")
    # 查询 多少个作业组 就起多少个子进程
    pool = multiprocessing.Pool(len(will_start_task_group_dict['task_group_id']))
    # 查询 多少个作业组 就起多少个子进程
    for task_group_id in range(1, (len(will_start_task_group_dict['task_group_id'])) + 1):
        pool.apply_async(_start_the_task_group_run_func, args=(will_start_task_group_dict['task_group_id'][task_group_id], task_group_id))
    pool.close()
    pool.join()

    run_time = will_start_task_group_dict['task_group_id'][1]['job_id'][1]['run_time']
    f = open(status_file_name, 'r+')
    file_list = f.readlines()
    f.close()

    #改状态
    for line in file_list:
        line2 = line.strip()
        line_list = line2.split(' ')

        ######直接写上，代表永远中
        # f.write(line)

        f = open(status_file_name, 'a')

        if line_list[0] == now_date and line_list[1] == run_time and line_list[2] == 'R':
            print(now_date, run_time, 'D', file=f)
            continue

    f.close()


##作业组执行程序 2.2 作业组并发多线程
def _start_the_task_group_run_func(job_id_dict, task_group_id):
    search_sql_obj = oracle_run_sql_class(223)
    search_sql = "select other_select_sql from t_Jobs_order where group_id = '%s' and job_id = 1" % (task_group_id)
    res = search_sql_obj.search_sql_func(search_sql)

    ##外部依赖
    pre_other_job_info = res[0][0]
    if pre_other_job_info == None:
        print(task_group_id, "no pre for other")
        pass
    else:
        search_sql_obj = oracle_run_sql_class(223)
        res = search_sql_obj.search_sql_func(pre_other_job_info)
        print('外部依赖作业情况', res[0][0])
        pre_other_job_flag = res[0][0]

        if pre_other_job_flag == 1:
            print("外部依赖作业未完成,exit!")
            sys.exit(0)

    # 定义线程池
    thread_list = []

    # 主进程开始
    # print("%s主线程正在运行...\n" % (threading.current_thread().name))


    #依次调用成功的存储过程
    global done_SP_set
    done_SP_set = set()

    #调度顺序列表
    global done_SP_list
    done_SP_list = []

    #应该调用的所有的存储过程
    global all_done_SP_set
    all_done_SP_set = set()

    for job_id in range(1, len(job_id_dict['job_id']) + 1):

        t = threading.Thread(target=__start_the_task_group_run_func, args=(job_id_dict['job_id'][job_id], task_group_id, job_id))
        t.start()
        thread_list.append(t)

    ##等待子进程或者线程结束后再继续往下运行（这时候只要线程 池中有线程，就接着等。。）
    for t in thread_list:
        t.join()

    print('顺序：', task_group_id, done_SP_list)

##作业组执行程序 2.3 作业组并发多线程
def __start_the_task_group_run_func(job_info, task_group_id, job_id):
    now_date_time = ((datetime.datetime.now()) + datetime.timedelta(days=-1)).strftime("%Y%m%d")
    start_time = (datetime.datetime.now()).strftime("%Y%m%d %H:%M:%S")

    ###对存储过程的传入参数进行整合
    ##存储过程 配置表存的参数
    list1 = job_info['para_info'].split(",")
    ##实际传入参数
    SP_para_info = []

    ###参数处理， API就在这里
    for line in list1:
        if line == 'i_data_date':
            SP_para_info.append(now_date_time)

    # 凑一下参数
    SP_para_info.append('ppppppppppppppppppppppppppppppppp')
    SP_para_info.append('ppppppppppppppppppppppppppppppppp')

    # #无依赖存储过程
    if job_info['prejob_id'] == None:
        # 调用存储过程

        try:
            #顺序表
            done_SP_list.append(job_id)
            call_storedprodure_obj = Call_StoredProcedure_Class(223)
            res = call_storedprodure_obj.Call_StoredProcedure(job_info['storeprodure_name'], SP_para_info)
            print('group_id:', task_group_id, 'job_id:', job_id, res)
            done_SP_set.add(job_id)

            ##写入日志
            run_status = 'D'
            end_time = (datetime.datetime.now()).strftime("%Y%m%d %H:%M:%S")
            insert_sql = "insert into t_jobs_logs(DATA_DAY,GROUP_ID,JOB_ID,PRO_NAME,RUN_START_TIME,RUN_END_TIME,RUN_STATUS,RUN_TIME) values('%s', %d, %d, '%s', to_date('%s','yyyymmdd hh24:mi:SS'), to_date('%s','yyyymmdd hh24:mi:SS'),'%s','%s')" % (
                now_date_time, task_group_id, job_id, job_info['storeprodure_name'], start_time, end_time, run_status, job_info['run_time']
            )
            sql_obj = oracle_run_sql_class(223)
            sql_obj.insert_into_sql_common_func(insert_sql)

        except Exception as e:
            print(e)
            print("调用存储过程出错！！！", job_info['storeprodure_name'], 'job_id是：', job_id)
            sys.exit(2)

    else: ##有依赖的
        while True:
            #满足依赖
            prejob_info = job_info['prejob_id']
            prejob_info_list = prejob_info.split(',')

            call_flag = True
            for l1 in prejob_info_list:
                if int(l1) in done_SP_set:
                    pass
                else:
                    call_flag = False

            if call_flag:
                # 顺序表
                done_SP_list.append(job_id)
                call_storedprodure_obj = Call_StoredProcedure_Class(223)
                try:
                    res = call_storedprodure_obj.Call_StoredProcedure(job_info['storeprodure_name'], SP_para_info)
                    print('group_id:', task_group_id, 'job_id:', job_id, res)
                    done_SP_set.add(job_id)

                    ##写入日志
                    end_time = (datetime.datetime.now()).strftime("%Y%m%d %H:%M:%S")
                    run_status = 'D'
                    insert_sql = "insert into t_jobs_logs(DATA_DAY,GROUP_ID,JOB_ID,PRO_NAME,RUN_START_TIME,RUN_END_TIME,RUN_STATUS,RUN_TIME) values('%s',%d,%d,'%s',to_date('%s','yyyymmdd hh24:mi:SS'),to_date('%s','yyyymmdd hh24:mi:SS'),'%s','%s')" % (
                    now_date_time, task_group_id, job_id, job_info['storeprodure_name'], start_time, end_time,
                    run_status, job_info['run_time'])
                    sql_obj = oracle_run_sql_class(223)
                    sql_obj.insert_into_sql_common_func(insert_sql)
                    break
                except Exception as e:
                    print(e)
                    # print("调用存储过程出错！！！", job_info['storeprodure_name'], 'job_id是：', job_id, 'task_group_id:', task_group_id)
                    sys.exit(2)
            else:
                # print('下一个循环。。。')
                time.sleep(3)
                print('group_id:', task_group_id, 'job_id:', job_id, '等待调度.........')

#主程序
def main_func():
    #取配置文件
    task_group_dict = get_config_json()

    #判断当前时间是否到了，到了就可以调用
    judge_the_task_group_run_func(task_group_dict)

if __name__ == '__main__':
    main_func()
    status_file_name = 'status.txt'

# if __name__ == '__main__':
#     global status_file_name
#     status_file_name = '/root/yunwei/test/call_SP_test/status.txt'
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