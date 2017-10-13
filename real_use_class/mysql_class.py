import MySQLdb

class mysql(object):
    def __init__(self):
        config_info_json = {
            'host': "172.18.126.51",
            'port': 3306,
            'db': 'geetest',
            'user': 'root',
            'password': 'Abcd1234'}
        self.conn = MySQLdb.connect(**config_info_json, charset='utf8')
        self.cursor = self.conn.cursor()

    def common_run_sql(self, sql):
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return result
        finally:
            self.conn.commit()

    def __del__(self):
        self.cursor.close()
        self.conn.close()
