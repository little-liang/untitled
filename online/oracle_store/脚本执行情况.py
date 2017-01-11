import cx_Oracle
import sys


db_223 = "dw/dw@10.138.22.223:1521/edw"
db_226 = "etl/etl_Haier@10.138.22.226:1521/edw"


conn = cx_Oracle.connect(db_223)
cur = conn.cursor()

result = cur.execute("select * from log_proc_exec_dw where time_stamp>='2016-12-23 10:22:32' order by seq desc")


# "select * from fcib.oh_FILEACSS where rownum <= 5"

for i in cur.description:
    i = i[0]
    i = "%-60s" % i
    sys.stdout.write(i)
print("")

for i in result:
    for line in range(len(i)):
        k = str(i[line])
        k = "%-60s" % k
        sys.stdout.write(k)
    print("")


conn.close()
