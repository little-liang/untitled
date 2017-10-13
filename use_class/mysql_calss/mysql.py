import MySQLdb

class mysql(object):
    """
    mysql 操作

    """
    def __init__(self):
        pass
    def common_run_sql(self, sql):
        config_info_json = {
        'host': "172.18.126.51",
        'port': 3306,
        'db': 'geetest',
        'user': 'root',
        'password': 'Abcd1234'}
        self.conn = MySQLdb.connect(**config_info_json, charset='utf8')
        self.cursor = self.conn.cursor()

        ## 待优化 每次查询都要连接断开,假设连接断开很慢,
        ## 最好本次程序运行完毕在进行数据库断开操作
        ##main 做成 上下文??
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return result
        finally:
            self.conn.commit()


    def __del__(self):
        self.cursor.close()
        self.conn.close()