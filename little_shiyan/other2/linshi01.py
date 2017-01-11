import cx_Oracle
import sys
import datetime
import time
import subprocess

conn = cx_Oracle.connect("etl/etl_Haier@10.138.22.226:1521/edw")
cur = conn.cursor()
# cur.execute(update_sql)
cur.execute("update ctl_fc set data_date = '20170108',update_time = to_date('2017/11/11 11:11:11','yyyy/mm/dd HH24:MI:SS') where system_id = 'fchry'")
conn.commit()
conn.close()





