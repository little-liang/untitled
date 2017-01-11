import cx_Oracle
import sys


db_223 = "etl/etl@10.138.22.223:1521/edw"
db_226 = "etl/etl_Haier@10.138.22.226:1521/edw"


conn = cx_Oracle.connect(db_226)
cur = conn.cursor()

result = cur.execute("select * from ctl_fc")

# E$_START_DATE = '20161225'
# "select * from fcib.oh_FILEACSS where rownum <= 5"

for i in cur.description:
    i = i[0]
    i = "%-25s" % i
    sys.stdout.write(i)
print("")

for i in result:
    for line in range(len(i)):
        k = str(i[line])
        k = "%-25s" % k
        sys.stdout.write(k)
    print("")

conn.close()
