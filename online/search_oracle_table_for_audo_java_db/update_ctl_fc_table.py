import cx_Oracle
import sys


db_223 = "etl/etl@10.138.22.223:1521/edw"
db_226 = "etl/etl_Haier@10.138.22.226:1521/edw"


conn = cx_Oracle.connect(db_226)
cur = conn.cursor()

result = cur.execute("update ctl_fc02 set data_date='20170325' where system_id in ('fchry', 'fccf') ")
conn.commit()
cur.close()
conn.close()