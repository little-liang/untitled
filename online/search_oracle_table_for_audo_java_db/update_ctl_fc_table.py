import cx_Oracle
import sys


db_223 = "etl/etl@10.138.22.223:1521/edw"
db_226_etl = "edwpdc/Haier_edwpdc@10.138.22.226:1521/edw"

conn = cx_Oracle.connect(db_226_etl)
cur = conn.cursor()


sql = "truncate table EDWPDC.EDWPDC_FINISH_FLAG"

result = cur.execute(sql)
conn.commit()
cur.close()
conn.close()