import os, subprocess




print(os.path.dirname(__file__))

Run_file = "%s/Auto_Call_SP_Run.py" % (os.path.dirname(__file__))

cmd = "python %s" % (Run_file)
print(cmd)
subprocess.run(cmd, check=True)