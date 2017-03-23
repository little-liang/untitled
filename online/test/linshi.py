import cx_Oracle
import datetime, time
import sys


#
# start_date = '2017-01-01'
#
# time_stamp = datetime.datetime.strptime(start_date, "%Y-%m-%d")
#
#
# print(time_stamp)
#
# time_stamp = time_stamp + datetime.timedelta(+35)
#
# # time_stamp = datetime.datetime.strftime(time_stamp, "%Y-%m-%d")
#
# time_stamp = datetime.datetime.strftime(time_stamp, "%Y%m%d")
#
# for line in range()


class Time_Translation_Class(object):
    def __init__(self, translate_time, begin_time_type, after_time_type, time_format):
        self.translate_time = translate_time
        self.begin_time_type = begin_time_type
        self.after_time_type = after_time_type
        self.time_format = time_format

    def String_Translate_Datetime_Func(self):
        after_translation_time = datetime.datetime.strptime(self.translate_time, self.time_format)
        return after_translation_time

    def String_Translate_TimeStruct_Func(self):
        after_translation_time = time.strptime(self.translate_time, self.time_format)
        return after_translation_time

    def TimeStruct_Translate_TimeSecondt_Func(self):
        after_translation_time = time.mktime(self.translate_time, self.time_format)


a = datetime.datetime.now()
print(a)