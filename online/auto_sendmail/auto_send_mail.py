import smtplib
import cx_Oracle
import datetime
from email.mime.text import MIMEText
from email.utils import formataddr
import sys, os, time


#查询标识表
def search_oracle_table_flag_func():
    conn = cx_Oracle.connect("etl/etl_Haier@10.138.22.226:1521/edw")
    cur = conn.cursor()
    result = cur.execute("select * from ctl_fc").fetchall()
    cur.close()
    conn.close()
    return result

#判断标识表
def judge_oracle_table_flag_func():

    auto_java_DB_flag = True
    result = search_oracle_table_flag_func()
    for l1 in result:
        if l1[1] != time_stamp:
            auto_java_DB_flag = False
            break
        else:
            pass
    return auto_java_DB_flag


#判断今天是否已经发了邮件
def sendmail_flag_today_func():

    #创建发送标识文件
    for l1 in os.listdir('/tmp'):
        if l1 == 'status.txt':
            break
    else:
        print("no status.txt ,will touch this file!")
        open('/tmp/status.txt', 'w')
        exit()

    with open("/tmp/status.txt", "r+") as f:
        sendmail_content = f.readline()
        if time_stamp == sendmail_content:
            sendmail_flag_today = True
            return sendmail_flag_today
        else:
            return False

#自动发邮件

def send_mail_func():

    #小判断
    sendmail_status_today = sendmail_flag_today_func()
    auto_java_DB_flag = judge_oracle_table_flag_func()

    if auto_java_DB_flag:
        pass
        print("auto java is ok ")
    else:
        print("auto java is not ok")
        exit()

    if sendmail_status_today:
        print("send mail alreday!, do not send")
        exit()

    show_time = result = search_oracle_table_flag_func()
    show_time_list = []

    for l1 in show_time:
        l2 = str(l1[2])
        l2 = time.strptime(l2, "%Y-%m-%d %H:%M:%S")
        l2 = time.mktime(l2)
        show_time_list.append(float(str(l2)))



    for l1 in range(len(show_time_list)):
        for l2 in range(l1, len(show_time_list)):
            if show_time_list[l1] < show_time_list[l2]:
                show_time_list[l1], show_time_list[l2] = show_time_list[l2], show_time_list[l1]

    show_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(show_time_list[0]))

    content = '''

    各位好：

           财务公司ods层

           今日调度已经全部完成

           调度完成时间：[%s]


    如有疑问，请及时沟通，谢谢！
    ------------------------------------------------------------------
    ODS数据运维组

    ''' % (show_time)

    msg = MIMEText(content, 'plain', 'utf-8')
    #这里是描述信息，收发件人是谁。
    msg['From'] = formataddr(["ODS数据运维组", 'liangyanlong@hydsoft.com'])
    msg['To'] = formataddr(["zhangchi", "zhangchi<zhangchi@hydsoft.com>,   zouying<zouying@hydsoft.com>,   xuyigang<xuyigang@hydsoft.com>,   yushaolong<yushaolong@hydsoft.com>,   lixiang<lixiang@hydsoft.com>,   sunzhen<sunzhen@hydsoft.com>"])
    msg['cc'] = formataddr(["liruimin", '<liruimin@haier.com>,   tangjr<tangjr@haier.com>,   lingxiaoke<lingxiaoke@haier.com>,   zhouzinan<zhouzinan@haier.com>,   zhubenxing<zhubenxing@hydsoft.com>,   liangyanlong<liangyanlong@hydsoft.com>,   zhuchaojie<zhuchaojie@hydsoft.com>,   chenpeng<chenpeng@hydsoft.com>'])
    msg['Subject'] = "ODS今日调度反馈"


    #真正的收发件人
    sender = 'liangyanlong@hydsoft.com'
    receivers = ['liangyanlong@hydsoft.com']
    #receivers = ['zhangchi@hydsoft.com', 'zouying@hydsoft.com', 'xuyigang@hydsoft.com', 'yushaolong@hydsoft.com', 'lixiang@hydsoft.com', 'sunzhen@hydsoft.com', 'liruimin@haier.com', 'tangjr@haier.com', 'lingxiaoke@haier.com', 'zhouzinan@haier.com', 'zhubenxing@hydsoft.com', 'liangyanlong@hydsoft.com', 'zhuchaojie@hydsoft.com', 'chenpeng@hydsoft.com']


    try:
        server = smtplib.SMTP("smtp.hydsoft.com", 25)
        server.login("liangyanlong@hydsoft.com", "Abcd1234")
        server.sendmail(sender, receivers, msg.as_string())
        server.quit()
        with open("/tmp/status.txt", "w") as f:
            f.write(time_stamp)
        print("send mail is ok!!!")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件")



if __name__ == '__main__':
    time_stamp = datetime.datetime.now() + datetime.timedelta(-1)
    time_stamp = time_stamp.strftime("%Y%m%d")
    send_mail_func()