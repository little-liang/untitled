import paramiko

task_content = 'ls /tmp'

task_content = ['df -h']

task_content = str(task_content)

print(task_content)
print(type(task_content))

task_content = task_content[2:-2]
print(task_content)
print(type(task_content))

# task_content_str = "".join(task_content)
# print(type(task_content_str))
# s = paramiko.SSHClient()
# s.load_system_host_keys()
# s.set_missing_host_key_policy(paramiko.AutoAddPolicy())
#
# s.connect('192.168.10.103',
#           22,
#           'root',
#           'Abcd@1234',
#           timeout=5)
#
# stdin, stdout, stderr = s.exec_command(task_content_str)
#
#
# #这里要求返回值只有一个，返回数据库里只能有一个
# result = stdout.read(), stderr.read()
# #一会看看
# print(result)