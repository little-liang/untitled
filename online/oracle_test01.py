import cx_Oracle
import sys


db_223 = "etl/etl@10.138.22.223:1521/edw"
# db_223 = "dw/dw@10.138.22.223:1521/edw"
db_226 = "etl/etl_Haier@10.138.22.226:1521/edw"


conn = cx_Oracle.connect(db_226)
cur = conn.cursor()

# result = cur.execute("select * from ETL.CTL_INCREMENT_ZCJ WHERE GROUP_ID = 'FCBI'")
result = cur.execute("select * from ctl_fc")

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
