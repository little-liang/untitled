import cx_Oracle
import sys


db_223 = "wsd/wsd@10.138.22.223:1521/edw"


conn = cx_Oracle.connect(db_223)
cur = conn.cursor()

result = cur.execute("delete from LOG_PROC_EXEC where proc_name='PACK_YANLONG_TEST.proc_all'")
conn.commit()
cur.close()
conn.close()