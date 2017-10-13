import pymysql as MySQLdb

class mysql(object):
    """
    mysql 操作

    """
    def __init__(self, host, user, passwd, db, port):
        self.conn = MySQLdb.connect(host=host, user=user, passwd=passwd, db=db, port=port, charset='utf8')
        self.cursor = self.conn.cursor()
    def common_run_sql(self, sql):


        ## 待优化 每次查询都要连接断开,假设连接断开很慢,
        ## 最好本次程序运行完毕在进行数据库断开操作
        ##main 做成 上下文??
        try:
            self.cursor.execute(sql)
            result = self.cursor.fetchall()
            return result
        finally:
            self.conn.commit()
            self.cursor.close()
            self.conn.close()


import urllib.parse
import subprocess
import sys
run_sql_obj = mysql('60.194.156.184', 'greech', 'greech123456', 'greech', 3123)
res = run_sql_obj.common_run_sql("select description,id from greech.t_cfg_domain where id between 510 and 600")
import time
for line in res:
    data3 = urllib.parse.quote(line[0].encode('gbk'))

    data3 = "word=%s&bs=%s" % (data3, data3)
    data3 = "http://news.baidu.com/ns?%s&sr=0&cl=2&rn=20&tn=news&ct=0&clk=sortbytime" % (data3)

    id2 = str(line[1]) + '0'
    data3 = """java -jar /data/dp/src/greech.jar test /data/dp/src/prop/greechrdb.prop /data/dp/backup %s '%s'""" % (id2, data3)

    print(data3)
    p = subprocess.Popen(data3, shell=True, stdout=subprocess.PIPE)
    sys.stdout.flush()
    print(p.stdout.readlines())
    time.sleep(0.1)

run_sql_obj = mysql('60.194.156.184', 'topic', 'topic12345', 'daping_news', 3123)
run_sql_obj.common_run_sql("delete from t_ctl_url")
