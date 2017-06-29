from optparse import OptionParser
import sys

#这是进行实例化处理

parser = OptionParser()

##add各种短长选项
parser.add_option("-t", "--task_group_id", action="store", dest="task_group_id", help="the task_group_id")
parser.add_option("-c", "--call_type_id", action="store", dest="call_type_id", help="the call_type_id")
parser.add_option("-d", "--now_date", action="store", dest="now_date", help="the now_date")
parser.add_option("-r", "--run_time", action="store", dest="run_time", help="the run_time", metavar="FILE")

# args = sys.argv[1:]
(options, args) = parser.parse_args(sys.argv[1:])
print(options)



'''
/usr/local/bin/python3 /server/scripts/call_SP_scripts/Auto_Call_SP_Run.py --task_group_id '1' --call_type_id '1' --now_date '20170530' --run_time '14:12'
'''