Run_file = "%s/Auto_Call_SP_Run.py" % (os.path.dirname(os.path.abspath(__file__)))
cmd = "python %s %s %s %s %s" % (Run_file, task_group_id, call_type_id, now_date, run_time)
print(cmd)