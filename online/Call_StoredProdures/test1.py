import cx_Oracle, datetime, time, sys, calendar


class oracle_run_sql_class(object):
    def __init__(self, oracle_DB):
        self.oracle_DB = oracle_DB
        if self.oracle_DB == 223:
            self.oracle_DB = "etc/etc@10.138.22.223:1521/edw"
        elif self.oracle_DB == 226:
            self.oracle_DB = "etc/etc_Control@10.138.22.226:1521/edw"

    def get_user_passwd(self, oracle_DB):
        if oracle_DB == 223:
            self.oracle_DB = "etc/etc@10.138.22.223:1521/edw"
        elif oracle_DB == 226:
            self.oracle_DB = "etc/etc_Control@10.138.22.226:1521/edw"
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
            return result[0]
        except BaseException as e:
            print(e)

    #插入更新表
    def insert_into_sql_common_func(self, insert_into_sql):
        try:
            conn = cx_Oracle.connect(self.oracle_DB)
            cur = conn.cursor()
            cur.execute(insert_into_sql)
            conn.commit()
            conn.close()
        except BaseException as e:
            print(e)

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
    def __init__(self,  Server_host):
        #返回值必须是这样的，这里写成了全局变量
        global func_return_code, func_return_message
        func_return_code = 'ppppppppppppppppppppppppppppppppppppppp'
        func_return_message = 'ppppppppppppppppppppppppppppppppppppppppppppppppppp'
        self.Server_host = Server_host
        if Server_host == 223:
            self.Server_host_id = self.Server_host_id = oracle_run_sql_class.get_user_passwd(oracle_run_sql_class, 223)
        elif Server_host == 226:
            self.Server_host_id = self.Server_host_id = oracle_run_sql_class.get_user_passwd(oracle_run_sql_class, 226)


    def run_done(self):
        now_date = (datetime.datetime.now()).strftime("%Y%m%d")
        search_sql_obj = oracle_run_sql_class(226)
        search_sql = "select DATA_DATE, RUN_STATUS from EDWPDC.EDWPDC_FINISH_FLAG where DATA_DATE = '%s' and RUN_STATUS ='0'" % (now_date)
        res = search_sql_obj.search_sql_func(search_sql)
        if res == None:
            return False
        if res[0] == now_date and res[1] == '0': return True
        else: return False

    # 调用外部依赖标识
    def check_before_task_flag_func(self):
        yesterday_date = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
        search_sql_obj = oracle_run_sql_class(226)
        search_sql = "select DATA_DATE, RUN_STATUS from fl.fl_view_flag where DATA_DATE = '%s' and RUN_STATUS ='0'" % (yesterday_date)
        res = search_sql_obj.search_sql_func(search_sql)
        if res[0] == yesterday_date and res[1] == '0': return True
        else: return False

    # 插入完成标识
    def insert_into_sql_func(self):
        now_date = (datetime.datetime.now()).strftime("%Y%m%d")
        now_date_time = (datetime.datetime.now()).strftime("%Y%m%d %H:%M:%S")
        insert_sql_obj = oracle_run_sql_class(226)
        insert_sql = "insert into EDWPDC.EDWPDC_FINISH_FLAG values('%s','0',to_date('%s','yyyymmdd hh24:mi:SS'))" % (
        now_date, now_date_time)
        try:
            insert_sql_obj.insert_into_sql_common_func(insert_sql)
        except Exception as e:
            print(e)

    #实际调存储过程代码
    def Call_StoredProcedure(self, StoredProcedure_Name, data_date):
        print("正在调用 存储过程[%s] 日期[%s] ..." % (StoredProcedure_Name, data_date))
        conn = cx_Oracle.connect(self.Server_host_id)
        cur = conn.cursor()
        res = cur.callproc(StoredProcedure_Name, [data_date, func_return_code, func_return_message])
        print(res, "\n")
        cur.close()
        conn.close()

    #业务常用1每日调用存储过程
    def EveryDay_Task_Func(self, *StoredProcedures_Name):
        day_now_datetime_str = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
        for SP_name in StoredProcedures_Name:
            self.Call_StoredProcedure(SP_name, day_now_datetime_str)

    #手动调度时间段
    def Maual_Task_Func(self, day_begin_string, day_end_string, *StoredProcedure_Name):

        day_begin_datetime = datetime.datetime.strptime(day_begin_string, "%Y%m%d")
        day_end_datetime = datetime.datetime.strptime(day_end_string, "%Y%m%d")
        delta = datetime.timedelta(days=1)
        data_date_tmp = day_begin_datetime


        #循环调取 存储过程
        while data_date_tmp <= day_end_datetime:
            data_date = data_date_tmp.strftime("%Y-%m-%d")

            #一个调度由多个存储过程组成
            if len(StoredProcedure_Name) > 1:
                for line in StoredProcedure_Name:
                    self.Call_StoredProcedure(line, data_date)
                data_date_tmp = data_date_tmp + delta

            # 一个调度由1个存储过程组成
            elif len(StoredProcedure_Name) == 1:
                for line in StoredProcedure_Name:
                    self.Call_StoredProcedure(line, data_date)
                data_date_tmp = data_date_tmp + delta


if __name__ == '__main__':
    duigong = Call_StoredProcedure_Class(226)
    if duigong.run_done():
        print("已经完成了， 退出")
        exit()

    if duigong.check_before_task_flag_func():

        try:
            print("外部依赖已经完成")
            duigong.EveryDay_Task_Func(
                'dw.pack_dw_all_new_ETL.pack_dw_all_new_FL',
                'dw.pack_dw_all_new_ETL.pack_dw_all_new_ML',
                'dw.pack_dw_all_new_ETL.pack_dw_all_new_FT',
                'dw.pack_dw_all_new_ETL.pack_dw_all_new_EDWPDC',
            )

            ##还差一个 写进去 调度 标识表
            duigong.insert_into_sql_func()
        except Exception as e:
            print(e)
    else:
        print('外部依赖未完成，退出')
        exit()

