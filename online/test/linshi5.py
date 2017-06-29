cmd = "ps -ef |grep 'Auto_Call_SP_Run.py'|grep '\-\-task_group_id %s \-\-call_type_id %s \-\-now_date %s \-\-run_time %s'|wc -l" % (task_group_id, call_type_id, now_date, run_time)
p = subprocess.run(cmd, check=True, shell=True, stdout=subprocess.PIPE)

print(p.stdout.decode().strip())