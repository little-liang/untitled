class oracle_run_sql_class(object):

    def __init__(self, oracle_DB):
        self.oracle_DB = oracle_DB
        if oracle_DB == 223:
            self.oracle_DB = "dw/dw@10.138.22.223:1521/edw"
        elif oracle_DB == 226:
            self.oracle_DB = "etl/etl_Haier@10.138.22.226:1521/edw"

    def get_user_passwd(self, oracle_DB):
        if oracle_DB == 223:
            self.oracle_DB = "dw/dw@10.138.22.223:1521/edw"
        elif oracle_DB == 226:
            self.oracle_DB = "etl/etl_Haier@10.138.22.226:1521/edw"
        return self.oracle_DB


class Call_StoredProcedure_Class(object):
    def __init__(self, Server_host):

        # 返回值必须是这样的，这里写成了全局变量
        global func_return_code, func_return_message
        func_return_code = ''
        func_return_message = '1234567890123456789012345'
        self.Server_host = Server_host
        if Server_host == 223:
            self.Server_host_id = oracle_run_sql_class.get_user_passwd(oracle_run_sql_class, 223)
        elif Server_host == 226:
            self.Server_host_id = oracle_run_sql_class.get_user_passwd(oracle_run_sql_class, 226)



aa = Call_StoredProcedure_Class(223)

print(aa.Server_host_id)

