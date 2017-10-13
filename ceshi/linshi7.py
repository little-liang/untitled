import MySQLdb

class mysql(object):
    def __init__(self):
        config_info_json = {
            'host': "60.194.156.184",
            'port': 3123,
            'db': 'news',
            'user': 'topic',
            'password': 'topic12345'}
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


import sys
run_sql_obj = mysql()
filename = 'company.txt'
with open(filename, 'r', encoding='utf-8') as f:
    for line in f:
        line = line.strip()
        sql = "select count(*) from p_news_02 where c_content like '{0}{1}{2}'".format("%", line, "%")
        print(sql)
        # count_num = run_sql_obj.common_run_sql(sql)
        # count_num = count_num[0][0]
        # if count_num > 100:
        #     sys.stdout.flush()
        #     print(line, count_num)