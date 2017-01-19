import cx_Oracle
import datetime
import sys

time_stamp = (datetime.datetime.now() + datetime.timedelta(-1))
time_stamp = time_stamp.strftime("%Y-%m-%d")
time_stamp = '2017-01-18'
print("正在跑...", time_stamp, "我只是显示...\n")
sys.stdout.flush()
func_return_code = ''
func_return_message = 'dddddddddddddddddddddddddddddddddddddddddd'

db_223 = "wsd/wsd@10.138.22.223:1521/edw"

conn = cx_Oracle.connect(db_223)
cur = conn.cursor()

res = cur.callproc('WSD.pack_wide_screen_display.proc_all', [time_stamp, func_return_code, func_return_message])
# res = cur.callproc('WSD.pack_inv_actual_value_zcj.proc_all', ['20170118', func_return_code, func_return_message])

print(res)
cur.close()
conn.close()