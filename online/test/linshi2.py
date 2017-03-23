import subprocess, sys
import os

print('E:\graduation_project\hosts\\backend\multi_task.py')
aa = 'E:\graduation_project\hosts\\backend\multi_task.py'

if os.path.isfile(aa):
    print("ok")
subprocess.run(['python', aa, '-task_id', '1111', '-run_type', 'by_paramiko'])

