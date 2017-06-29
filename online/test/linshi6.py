import cx_Oracle
class Call_StoredProcedure_Class(object):
    def __init__(self, Server_host):
        self.Server_host = Server_host
        if Server_host == 223:
            self.Server_host_id = "hry_exp/hry_exp@10.138.22.223:1521/edw"
        elif Server_host == 226:
            self.Server_host_id = "hry_exp/hry_exp@10.138.22.223:1521/edw"

    def Before_Call_StoredProcedure(self, StoredProcedure_Name_list):
        pass

        # 调取存储过程代码
    def Call_StoredProcedure(self, StoredProcedure_Name, para_list):
        print("kkk", StoredProcedure_Name, para_list)

        try:
            conn = cx_Oracle.connect(self.Server_host_id)
            cur = conn.cursor()
            res = cur.callproc(StoredProcedure_Name, para_list)
            cur.close()
            conn.close()
            print(res)
            return res

        except Exception as e:
            print(e)
para_list = []
para_list.append('oh_hry_lcyw' )

#调用存储过程
pp = Call_StoredProcedure_Class(223)


pp.Call_StoredProcedure('hry_exp.pkg_de_duplication.pro_main', ['oh_hry_lcyw','20170101','ff', 'gggggggggg'])
