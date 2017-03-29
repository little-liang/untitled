import cx_Oracle
import datetime, time
import sys
import calendar

class Call_StoredProcedure_Class(object):
    def __init__(self,  Server_host):

        #返回值必须是这样的，这里写成了全局变量
        global func_return_code, func_return_message
        func_return_code = ''
        func_return_message = '1234567890123456789012345'
        self.Server_host = Server_host
        if Server_host == 223:
            self.Server_host_id = 'wsd/wsd@10.138.22.223:1521/edw'
        elif Server_host == 226:
            self.Server_host_id = 'wsd/wsd@10.138.22.226:1521/edw'

    #大屏 每日 调度动作
    def EveryDay_Task_Main_Func(self, day_begin_string, day_end_string, *StoredProcedure_Name):

        day_begin_datetime = datetime.datetime.strptime(day_begin_string, "%Y%m%d")
        day_end_datetime = datetime.datetime.strptime(day_end_string, "%Y%m%d")
        delta = datetime.timedelta(days=1)
        data_date_tmp = day_begin_datetime

        print()


        # #循环调取 存储过程
        while data_date_tmp < day_end_datetime:
            data_date = data_date_tmp.strftime("%Y-%m-%d")

            #一个调度由多个存储过程组成
            if len(StoredProcedure_Name) > 1:
                for line in StoredProcedure_Name:
                    self.Call_StoredProcedure(line, data_date)
                data_date_tmp = data_date_tmp + delta

            # 一个调度由1个存储过程组成
            elif len(StoredProcedure_Name) == 1:
                pass


    #调取存储过程代码
    def Call_StoredProcedure(self, StoredProcedure_Name, data_date):
        print("正在调用 存储过程[%s] 日期[%s] ..." % (StoredProcedure_Name, data_date))
        # conn = cx_Oracle.connect(self.Server_host_id)
        # cur = conn.cursor()
        # res = cur.callproc(StoredProcedure_Name, [data_date, func_return_code, func_return_message])
        # print(res, "\n")
        # cur.close()
        # conn.close()

    #大屏 18:00 特殊调度动作
    def _special(self):
        day_now_struct = time.localtime()

        #月初时间 %d-%02d-01 这个是
        day_begin_string = '%d-%02d-01' % (day_now_struct.tm_year, day_now_struct.tm_mon)
        day_now_string = '%d-%02d-%02d' % (day_now_struct.tm_year, day_now_struct.tm_mon, day_now_struct.tm_mday)

        #返回值 第一个 周几 ，第二个一个月的天数
        wday, monthRange = calendar.monthrange(day_now_struct.tm_year, day_now_struct.tm_mon)
        day_end_string = '%d-%02d-%02d' % (day_now_struct.tm_year, day_now_struct.tm_mon, monthRange)

        #月初不做工作
        if day_now_string == day_begin_string:
            print("It's the month start date [%s], don't call the StoredProcedure, will eixt!" % (day_now_string))

        day_begin_datetime = datetime.datetime.strptime(day_begin_string, "%Y-%m-%d")
        day_now_datetime = datetime.datetime.strptime(day_now_string, "%Y-%m-%d")
        delta = datetime.timedelta(days=1)
        data_date_tmp = day_begin_datetime

        #循环调取 存储过程
        while data_date_tmp < day_now_datetime:
            data_date = data_date_tmp.strftime("%Y-%m-%d")
            self.Call_StoredProcedure(data_date)
            data_date_tmp = data_date_tmp + delta



if __name__ == '__main__':
    duigong = Call_StoredProcedure_Class(223)
    duigong.EveryDay_Task_Main_Func('20170101', '20170103', "a1", 'a2', 'a3')