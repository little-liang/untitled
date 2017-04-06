import cx_Oracle
import sys, time


db_223 = "etl/etl@10.138.22.223:1521/edw"
# db_223 = "dw/dw@10.138.22.223:1521/edw"
db_226_wsd = "wsd/wsd@10.138.22.226:1521/edw"
db_226_etl = "etl/etl_Haier@10.138.22.226:1521/edw"

# for i in range(1, 100):
conn = cx_Oracle.connect(db_223)
cur = conn.cursor()
# sql = "select lower(proc_param),begin_time, t.* from LOG_PROC_EXEC t where proc_name='pack_frame_load_file.proc_main' order by t.run_time desc"
# sql = "select * from log_proc_exec where proc_name = 'pack_wide_screen_display.proc_all' and end_time > '2017-04-01 15:00:00' and end_time < '2017-04-02 20:40:00' order by end_time desc"
# sql = "select proc_param,run_time from LOG_PROC_EXEC t where proc_name='pack_frame_load_file.proc_main'"
# sql = "select * from ctl_fc_time where system_id in ('fchry', 'fccf') ORDER by start_date desc"
sql = "select * from etl.t_Jobs_Order"



cur.execute(sql)
result = cur.fetchall()

#字段名
for i in cur.description:
    i = i[0]
    i = "%-40s" % i
    sys.stdout.write(i)
print("")

#数据
for i in result:
    for line in range(len(i)):
        k = str(i[line])
        k = "%-40s" % k
        sys.stdout.write(k)
    print("")

cur.close()
conn.close()