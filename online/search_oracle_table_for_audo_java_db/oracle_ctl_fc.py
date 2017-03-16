import cx_Oracle
import sys, time


db_223 = "etl/etl@10.138.22.223:1521/edw"
# db_223 = "dw/dw@10.138.22.223:1521/edw"
db_226 = "etl/etl_Haier@10.138.22.226:1521/edw"

for i in range(1, 100):
    conn = cx_Oracle.connect(db_226)
    cur = conn.cursor()

    # result = cur.execute("select lower(proc_param),begin_time, t.* from LOG_PROC_EXEC t where proc_name='pack_frame_load_file.proc_main' order by t.run_time desc")
    # result = cur.execute("select * from ctl_fc_time where data_date='20170315'")
    result = cur.execute("select proc_param,run_time from LOG_PROC_EXEC t where proc_name='pack_frame_load_file.proc_main' and exec_date=20170315 and run_time>50 order by run_time desc")

    #W02_STA_ASSETS_BALANCE
    for i in cur.description:
        i = i[0]
        i = "%-40s" % i
        sys.stdout.write(i)
    print("")

    for i in result:
        for line in range(len(i)):
            k = str(i[line])
            k = "%-40s" % k
            sys.stdout.write(k)
        print("")

    conn.close()

    time.sleep(10)