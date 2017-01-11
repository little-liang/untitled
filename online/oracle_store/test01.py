import  cx_Oracle
#
db_223 = "dw/dw@10.138.22.223:1521/edw"
#
#
# for date in range(20161101, 20161104):
#     print(date)
#     conn = cx_Oracle.connect(db_223)
#     cur = conn.cursor()
#     proc_name = 'DW'
#     func_return_code = "111"
#     func_return_message = '1111111111'
#
#     res = cur.callproc('dw.pack_dw_all.proc_main', [date, proc_name, func_return_code, func_return_message])
#
#     print(res)
#
#     cur.close()
#     conn.close()
#     print("999999")


conn = cx_Oracle.connect(db_223)
cur = conn.cursor()

proc_name1 = 'DW'
proc_name2 = 'EDWPDC'

res = cur.callproc('dw.pack_dw_all.proc_main', [date, proc_name, func_return_code, func_return_message])

dw.pack_dw_all.proc_main,[i_data_date,i_type1,v_return_code,v_return_message]
dw.pack_dw_update_01.proc_dw_update,[i_data_date ,v_return_code,v_return_message]
dw.pack_dw_all.proc_main[i_data_date,i_type2,v_return_code,v_return_message]