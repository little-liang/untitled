import cx_Oracle, datetime
import sys


db_223 = "etc/etc@10.138.22.223:1521/edw"
db_226 = "etc/etc_Control@10.138.22.226:1521/edw"
# db_226 = "etl/etl_Haier@10.138.22.226:1521/edw"
db_226_etl = "edwpdc/Haier_edwpdc@10.138.22.226:1521/edw"

conn = cx_Oracle.connect(db_226)
cur = conn.cursor()

now_date = (datetime.datetime.now() + datetime.timedelta(days=-1)).strftime("%Y%m%d")
print(now_date)
sql = "update  t_Jobs_Frequency  set RUN_TIME ='06:00' where GROUP_ID  = '3'"
result = cur.execute(sql)
print(result)
conn.commit()
cur.close()
conn.close()