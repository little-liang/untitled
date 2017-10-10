import time
import threading


def sub_thread():
    print('\t子线程%s正在运行...' % (threading.current_thread().name))

    print("\t子线程%s完成部件" % (threading.current_thread().name))


    print('\t子线程%s已经完成工作!\n' % (threading.current_thread().name))
    time.sleep(2)

#定义线程池
thread_list = []

#主进程开始
print("%s主线程正在运行...\n" % (threading.current_thread().name))
for i in range(4):
    t = threading.Thread(target=sub_thread)
    t.start()
    thread_list.append(t)

##等待子进程或者线程结束后再继续往下运行（这时候只要线程 池中有线程，就接着等。。）
for t in thread_list:
    t.join()

print("%s主线程,done!!!" % (threading.current_thread().name))