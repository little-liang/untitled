import cx_Oracle


func_return_code = 'pppppppppppppppppppppppppppppp'
func_return_message = 'ppppppppppppppppppppppppppppppppp'
data_date = '20170418'
Server_host_id = 'etc/etc@10.138.22.223:1521/edw'

StoredProcedure_Name = 'dw.pack_dw_all_new_ETL.pack_dw_all_new_FL'

print("正在调用 存储过程[%s] 日期[%s] ..." % (StoredProcedure_Name, data_date))
conn = cx_Oracle.connect(Server_host_id)
cur = conn.cursor()
res = cur.callproc(StoredProcedure_Name, [data_date, func_return_code, func_return_message])
print(res, "\n")
cur.close()
conn.close()
