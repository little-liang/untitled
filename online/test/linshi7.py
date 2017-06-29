import json, os, re, datetime, cx_Oracle, subprocess, threading


# oracle操作类
class oracle_run_sql_class(object):
    db223_connect_info = "hry_exp/hry_exp@10.138.22.223:1521/edw"
    db226_connect_info = "hry_exp/Haier_hry_exp@10.138.22.226:1521/edw"

    def __init__(self, which_db):
        self.which_db = which_db
        if which_db == 223:
            self.which_db = oracle_run_sql_class.db223_connect_info
        elif which_db == 226:
            self.which_db = oracle_run_sql_class.db226_connect_info

    # 查询 表结构
    def search_sql_description(self, search_sql):
        col_description = []
        try:
            conn = cx_Oracle.connect(self.which_db)
            cur = conn.cursor()
            cur.execute(search_sql)
            result = cur.description
            cur.close()
            conn.close()
            for line in result:
                line2 = line[0]
                col_description.append(line2)
            return col_description
        except BaseException as e:
            print(e)
            print("Oracle search connect is broken!!!", e)

    # 查询方法
    def search_sql_func(self, search_sql):

        try:
            conn = cx_Oracle.connect(self.which_db)
            cur = conn.cursor()
            cur.execute(search_sql)
            result = cur.fetchall()
            cur.close()
            conn.close()
            return result
        except BaseException as e:
            print(e)
            print("Oracle search connect is broken!!!", e)

    # 插入通用版本
    def insert_into_sql_common_func(self, insert_sql):
        try:
            conn = cx_Oracle.connect(self.which_db)
            cur = conn.cursor()
            cur.execute(insert_sql)
            conn.commit()
            conn.close()
        except BaseException as e:
            print(e)
            print("Oracle connect is broken!!!")

    # 删除通用版本
    def delete_sql_common_func(self, delete_sql):
        try:
            conn = cx_Oracle.connect(self.which_db)
            cur = conn.cursor()
            cur.execute(delete_sql)
            conn.commit()
            conn.close()
        except BaseException as e:
            print(e)
            print("delete Oracle connect is broken!!!")


# 调用存储过程
class Call_StoredProcedure_Class(object):
    def __init__(self, Server_host):
        self.Server_host = Server_host
        if Server_host == 223:
            self.Server_host_id = oracle_run_sql_class.db223_connect_info
        elif Server_host == 226:
            self.Server_host_id = oracle_run_sql_class.db226_connect_info

    def Before_Call_StoredProcedure(self, StoredProcedure_Name_list):
        pass

        # 调取存储过程代码
    def Call_StoredProcedure(self, StoredProcedure_Name, para_list):
        try:
            conn = cx_Oracle.connect(self.Server_host_id)
            cur = conn.cursor()
            res = cur.callproc(StoredProcedure_Name, para_list)
            cur.close()
            conn.close()
            return res
        except Exception as e:
            print(e)


##取程序配置文件信息
def pro_config(config_file_path):
    # 设置以utf-8解码模式读取文件，encoding参数必须设置，否则默认以gbk模式读取文件，当文件中包含中文时，会报错
    f = open(config_file_path, encoding='utf-8')
    config_json = json.load(f)
    return config_json


##sqlplus配置文件自动成
def write_config_file(file_name, file_path):
    # 表文件位置
    _abs_file_path = file_path

    ##表名
    _table_name = re.split('\_|\.', file_name)[1]

    ##分隔符
    _separator = config_json['separator']
    _separator2 = config_json['separator2']

    ##字段名子,从数据库中读出来表结构
    db226_obj = oracle_run_sql_class(226)
    search_sql = "select * from oh_hry_%s" % (_table_name)
    _colume_name = db226_obj.search_sql_description(search_sql)
    _colume_name = str(_colume_name).strip("[]")
    _colume_name = _colume_name.replace('\'', '')

    # 写入文件

    _ctl_file_name_path = file_name.split(".")[0] + '.ctl'
    _abs_ctl_file_name_path = "%s/%s/%s" % (dst_file_path, yesterday_date, _ctl_file_name_path)
    with open(_abs_ctl_file_name_path, 'w') as f:
        print("options (bindsize=100000,silent=(feedback, errors))", file=f)
        print("load data", file=f)
        print("CHARACTERSET UTF8", file=f)
        print("infile '%s'" % (_abs_file_path), file=f)
        print("append into table oh_hry_%s" % (_table_name), file=f)
        print("fields terminated by '%s'" % (_separator), file=f)
        print("optionally enclosed by '%s'" % (_separator2), file=f)
        print("TRAILING NULLCOLS", file=f)
        f.write('(%s)' % (_colume_name))

    return "oh_hry_%s" % (_table_name)


# 查处所有的文件列表, 可并发
def display_all_file_list(dst_file_path):
    dst_file_path = "%s/%s" % (dst_file_path, yesterday_date)
    all_file_list = os.listdir(dst_file_path)
    table_name_list = []

    for file_name in all_file_list:

        if file_name.endswith("csv"):
            file_path = "%s/%s" % (dst_file_path, file_name)

            ##写入配置文件sqlplus配置文件自动成
            table_name = write_config_file(file_name, file_path)
            table_name_list.append(table_name)

    return table_name_list


# 给海融易文件转码 调用dos2unix命令 默认直接调用 直接提换
def translation_char(config_json):
    file_path = "%s/%s" % (config_json['dst_file_path'], yesterday_date)

    for line in os.listdir(file_path):
        if line.endswith('csv'):
            abs_file_path = "%s/%s" % (file_path, line)

            # translation
            try:
                cmd = "/usr/local/bin/dos2unix -U %s" % (abs_file_path)
                subprocess.run(cmd, shell=True)
            except Exception as e:
                print("translation fail! check %s" % (cmd))


##调用的导入命令
def sqlldr_load(file_path):
    print("要同时出现")
    log_file2 = file_path.split(".")[0]
    log_file2 = log_file2.split("/")[-1]
    log_file2 = log_file2 + ".log"

    log_file3 = "%s/%s" % (log_file, log_file2)

    cmd = "/u01/app/oracle/product/11.2.0/dbhome_1/bin/sqlldr %s silent=feedback,header control=%s skip=1 log=%s" % (
        oracle_run_sql_class.db226_connect_info,
        file_path,
        log_file3

    )
    # load
    try:
        subprocess.run(cmd, shell=True)
        print("导入完成")
    except Exception as e:
        print("load fail! check %s" % (cmd))


##调用sqlldr 导入 文本
def load_file(config_json):
    file_path = "%s/%s" % (config_json['dst_file_path'], yesterday_date)
    thread_list = []
    for line in os.listdir(file_path):
        if line.endswith('ctl'):
            abs_file_path = "%s/%s" % (file_path, line)
            t = threading.Thread(target=sqlldr_load, args=(abs_file_path,))
            t.start()
            thread_list.append(t)
    for t in thread_list:
        t.join()


if __name__ == '__main__':

    yesterday_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime("%Y%m%d")
    # 程序配置
    config_file_path = "/root/yunwei/mv_hrydata_scripts/hry.config"
    log_file = "/var/log/load_hrydata/load_action_226"
    config_json = pro_config(config_file_path)

    # 查处所有的文件列表,  可并发, 并且写入配置文件sqlplus配置文件自动成
    ##
    dst_file_path = config_json['dst_file_path']

    ##这里可以拿出所有的表列表
    table_name_list = display_all_file_list(dst_file_path)

    # 给海融易文件转码 调用dos2unix命令 默认直接调用 直接提换
    translation_char(config_json)

    # 调用sqlldr 导入 文本
    load_file(config_json)

    # 调用存储过程去重, 放置多次注入
    print("开始去重")
    call_SP_obj = Call_StoredProcedure_Class(226)
    for table_name in table_name_list:
        para_list = []
        para_list.append(table_name)
        para_list.append(yesterday_date)
        para_list.append('fffffffffffffff')
        para_list.append('fffffffffffffffffffffff')
        call_SP_obj.Call_StoredProcedure("hry_exp.pkg_de_duplication.pro_main", para_list)