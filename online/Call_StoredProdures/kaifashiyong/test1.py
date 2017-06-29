import cx_Oracle
import datetime, time
import sys



db226_etc = 'etc/etc_Crontal@10.138.22.226:1521/edw'
db223_etc = 'etc/etc@10.138.22.223:1521/edw'
db226_dw = 'dw/Haier_dw@10.138.22.226:1521/edw'

data_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")



para_list = []
para_list.append(data_date)
tmp_code = 'pppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppppp'
para_list.append(tmp_code)
para_list.append(tmp_code)

def foo(func):
    def _deco(arg1, arg2):
        start_time = time.time()
        ret = func(arg1, arg2)
        end_time = time.time()
        cost_time = end_time - start_time
        cost_time = round(cost_time / 60, 2)
        print(cost_time)
        return ret
    return _deco

@foo
def call_SP(StoredProcedure_Name, para_list):
    print('call', StoredProcedure_Name, '...')

    conn = cx_Oracle.connect(db226_dw)
    cur = conn.cursor()
    res = cur.callproc(StoredProcedure_Name, para_list)
    cur.close()
    conn.close()
    print(res)
    return res

call_SP("dw.pack_dw_all_new_etl.pack_dw_all_new_single_01", para_list)