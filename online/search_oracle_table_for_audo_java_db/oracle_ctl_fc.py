import cx_Oracle
import sys, time


db_223 = "etc/etc@10.138.22.223:1521/edw"
db_226_etc = "etc/etc_Control@10.138.22.226:1521/edw"
db_2233 = "ml/ml@10.138.22.226:1521/edw"
db223_hry = "hry_exp/hry_exp@10.138.22.223:1521/edw"
# db_223 = "dw/dw@10.138.22.223:1521/edw"
db_226_wsd = "wsd/wsd@10.138.22.226:1521/edw"
db_226_etl = "etl/etl_Haier@10.138.22.226:1521/edw"
conn = cx_Oracle.connect(db_226_etl)
cur = conn.cursor()
# sql = "select lower(proc_param),begin_time, t.* from LOG_PROC_EXEC t where proc_name='pack_frame_load_file.proc_main' order by t.run_time desc"
# sql = "select * from log_proc_exec where proc_name = 'pack_wide_screen_display.proc_all' and end_time > '2017-04-01 15:00:00' and end_time < '2017-04-02 20:40:00' order by end_time desc"
# sql = "select proc_param,run_time from LOG_PROC_EXEC t where proc_name='pack_frame_load_file.proc_main'"
# sql = "select * from ctl_fc_time where system_id in ('fchry', 'fccf') ORDER by start_date desc"
# sql = "select other_select_sql from t_Jobs_order where group_id = '2'"
# sql = "select decode(RUN_STATUS,0,0,1) from fl.fl_view_flag where data_date = to_char(sysdate,'yyyymmdd')-3"
# sql = "select * from EDWPDC.EDWPDC_FINISH_FLAG"
# sql = "select * from ctl_fc_time where SYSTEM_ID = 'fcib'"
# sql = "select SOURCE_TABLE_NAME  from etl.ctl_core_zcj where GROUP_id ='FCSBS'"
sql = "select * from etl.ctl_fc"
#sql = "select * from t_Jobs_Frequency"
# sql = "select * from  v$asm_disk"
# sql = "select * from dw.dw_dependences"
# sql = "select * from dba_data_files"
#psql =select * from LOG_PROC_EXEC_DW order by seq desc; ""

#sql = "select * from wsd.T_BATCH_FLAG_INCOME t where batch_date=to_char(sysdate-1,'YYYYMMDD')"
# sql = "select * from DW.WD_AG_LOAN_REPAYMENT_PLAN where rownum <= 3"
# sql = "select PROC_PARAM ,EXEC_DATE, RETURN_MESSAGE  from dw.log_proc_exec_dw  where exec_date = '20170531' and proc_param = '20170531 | FL' or proc_param = '20170531 | FT' or proc_param = '20170531 | ML' or proc_param = '20170531 |EDWPDC'"
# sql = "select PROC_PARAM, EXEC_DATE, RETURN_MESSAGE from dw.log_proc_exec_dw t where exec_date = to_char(sysdate - 1, 'yyyymmdd') and proc_param = to_char(sysdate - 1, 'yyyymmdd') ||' | EDWPDC'"

cur.execute(sql)
result = cur.fetchall()

#字段名
for i in cur.description:
    i = i[0]
    i = "%-60s" % i
    sys.stdout.write(i)
print("")

#数据
for i in result:
    for line in range(len(i)):
        k = str(i[line])
        k = "%-60s" % k
        sys.stdout.write(k)
    print("")

cur.close()
conn.close()