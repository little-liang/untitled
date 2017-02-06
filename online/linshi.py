import cx_Oracle
import datetime
import sys
import time

time_stamp = (datetime.datetime.now() + datetime.timedelta(-1))
time_stamp = time_stamp.strftime("%Y-%m-%d")
time_stamp = '2017-01-20'
print("will start data_time:", time_stamp)

start_time = time.time()
start_time2 = time.strftime("%Y-%m-%d %H:%M:%S")
print("start time:::", start_time2)
sys.stdout.flush()

func_return_code = ''
func_return_message = '1234567890123456789012345'

db_226 = "wsd/wsd@10.138.22.226:1521/edw"

conn = cx_Oracle.connect(db_226)
cur = conn.cursor()

try:
    res = cur.callproc('WSD.pack_wide_screen_display.proc_all', [time_stamp, func_return_code, func_return_message])
except:
    print("worrg!!")
    sys.exit()

print(res)
cur.close()
conn.close()

end_time = time.time()
end_time2 = time.strftime("%Y-%m-%d %H:%M:%S ")
print("end_time:::", end_time2)

cost_time = end_time - start_time
cost_time = round(cost_time/60, 2)
print("total minute:", cost_time)