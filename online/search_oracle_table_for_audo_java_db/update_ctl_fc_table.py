import cx_Oracle
import sys


db_223 = "etl/etl@10.138.22.223:1521/edw"


conn = cx_Oracle.connect(db_223)
cur = conn.cursor()

result = cur.execute("update ctl_fc set data_date = '20161231' where system_id = 'fchry'")
conn.commit()
cur.close()
conn.close()