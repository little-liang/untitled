import cx_Oracle
import sys


db_223 = "etl/etl@10.138.22.223:1521/edw"
db_226_etl = "etl/etl_Haier@10.138.22.226:1521/edw"

conn = cx_Oracle.connect(db_223)
cur = conn.cursor()

job_info = {}

now_date_time = '20170101'
task_group_id = 1
job_id = 1
job_info['storeprodure_name'] = 'ggg'
start_time = 'dddd'
end_time = 'fff'
run_status = 'fff'
job_info['run_time'] = 'fff'



sql = "insert into etl.t_jobs_logs(DATA_DAY,GROUP_ID,JOB_ID,PRO_NAME, RUN_START_TIME,RUN_END_TIME,RUN_STATUS, RUN_TIME)values(to_date(20170101,'yyyymmdd'),1,1,'ffff',to_date('20161102 10:03','yyyymmdd hh24:mi'),to_date('20161102 10:03','yyyymmdd hh24:mi'),'D','09:00')"

result = cur.execute(sql)
conn.commit()
cur.close()
conn.close()