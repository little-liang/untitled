import subprocess,os

cmd = "ps -ef|grep java|egrep -v 'u01|sh|py'"

print(cmd)
# # 检查shell命令是否能够执行
try:
    p = subprocess.run(cmd, shell=True, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
    print(p.decode())

except subprocess.CalledProcessError:
    print("The command [%s] wrong, will exit!" % (cmd))
    os._exit()

print("shell ok!")


# 查跑批进程是否正在运行
# print("%s %s" % (self.name, update_table_time))
# if ps_status.__contains__(self.name) and ps_status.__contains__(update_table_time):
#     print("ok")

os._exit()