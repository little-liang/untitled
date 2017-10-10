import cx_Oracle
import sys


db_223 = "etl/etl@10.138.22.223:1521/edw"
db_226 = "etl/etl_Haier@10.138.22.226:1521/edw"


conn = cx_Oracle.connect(db_226)
cur = conn.cursor()

cur.execute("update ctl_fc set data_date='20170116',update_time=to_date('2011/11/11 11:11:11','yyyy/mm/dd HH24:MI:SS') where system_id='fccf'")
conn.commit();
cur.close()
conn.close()
