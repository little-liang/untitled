import cx_Oracle, time, datetime, sys

class call_db_store_class(object):
    time_stamp = (datetime.datetime.now() + datetime.timedelta(-1))
    time_stamp = time_stamp.strftime("%Y%m%d")

    def __init__(self, oracle_DB):
        if oracle_DB == 223:
            self.oracle_DB = "dw/dw@10.138.22.223:1521/edw"
        elif oracle_DB == 226:
            self.oracle_DB = "dw/dw@10.138.22.226:1521/edw"
        else:
            print("the DB no exist, will exit!")
            exit()

    def dw_main_func(self, which_DB, data_date=time_stamp, proc_name="dw.pack_dw_all.proc_main", return_code="111", return_message="11111111"):
        conn = cx_Oracle.connect(self.oracle_DB)
        cur = conn.cursor()
        res = cur.callproc(proc_name, [data_date, which_DB, return_code, return_message])
        cur.close()
        conn.close()
        print(res)

    def for_dw_main_func(self, which_DB, start_data_date, end_data_date,proc_name="dw.pack_dw_all.proc_main", return_code="111", return_message="11111111"):

        for l1 in range(start_data_date, end_data_date + 1):
            conn = cx_Oracle.connect(self.oracle_DB)
            cur = conn.cursor()
            res = cur.callproc(proc_name, [l1, which_DB, return_code, return_message])
            cur.close()
            conn.close()
            print(res)
            print(l1)
        else:
            print("done!!!")



    def dw_part_1_func(self, data_date=time_stamp, proc_name="dw.pack_dw_update_01.proc_dw_update", return_code="1111", return_message="1111111111111"):
        conn = cx_Oracle.connect(self.oracle_DB)
        cur = conn.cursor()
        res = cur.callproc(proc_name, [data_date, return_code, return_message])
        cur.close()
        conn.close()
        print(res)


    def dw_part_2_func(self, data_date=time_stamp, proc_name="dw.pack_dw_update_02.proc_zip_update", return_code="1111", return_message="1111sdafsdfaafafa111111111"):
        return_message=tmp
        conn = cx_Oracle.connect(self.oracle_DB)
        cur = conn.cursor()
        res = cur.callproc(proc_name, [data_date, return_code, return_message])
        cur.close()
        conn.close()
        print(res)

    def search_result(self):
        conn = cx_Oracle.connect(self.oracle_DB)
        cur = conn.cursor()
        res = cur.execute("select * from dw.log_proc_exec_dw order by seq desc")

        for i in cur.description:
            i = i[0]
            i = "%-25s" % i
            sys.stdout.write(i)
        print("")

        for i in res:
            for line in range(len(i)):
                k = str(i[line])
                k = "%-25s" % k
                sys.stdout.write(k)
            print("")
        cur.close()
        conn.close()


tmp = 'qqqq'
a = 'sdfsdf'
for i in range(100):
    tmp += a

print(len(tmp))

#这是代表往223上运行
dw_main = call_db_store_class(223)

#dw --》edwpdc 3部曲
dw_main.dw_main_func("dw")
#dw_main.dw_part_1_func()
dw_main.dw_main_func("edwpdc")
# dw_main.dw_part_2_func()



#这个支持dw的批量跑批
#dw_main.for_dw_main_func("dw", 20170101, 20170110)