import time
import threading


def sub_thread():
    print('\t子线程%s正在运行...' % (threading.current_thread().name))


    for line in range(1, 5):
        print("\t子线程%s完成%s号部件" % (threading.current_thread().name, line))
        time.sleep(2)

    print('\t子线程%s已经完成工作!\n' % (threading.current_thread().name))



print("%s主线程正在运行...\n" % (threading.current_thread().name))
t1 = threading.Thread(target=sub_thread)

##将主线程设置为Daemon线程,它做为程序主线程的守护线程,当主线程退出时,子线程会立即退出，无论是否完成任务，（不用守护进程，子线程依旧在运行）
#这个对应主线程，join方法再用，目前不知道守护进程模式有啥用
t1.setDaemon(True)
t1.start()
#join()方法可以等待子进程或者线程结束后再继续往下运行，通常用于进程间的同步,在这里就成了依次进行，无并发，去掉join就是并发了
#去点后，是主线程执行完后，子线程还在运行，这样不合逻辑，
#理想的是，主线程开始，子线程同步进行，子进程结束后，主线程结束，不用线程池，解决不了这个问题
# t1.join()
print("%s主线程,done!!!" % (threading.current_thread().name))


#上面根本不是多并发，下面的是并发，但是还是解决不了理想的问题，
#for 就是为了凑循环次数
# for line in range(5):
#     print("%s主线程正在运行...\n" % (threading.current_thread().name))
#     print(line)
#     t = threading.Thread(target=sub_thread)
#     t.start()
#     # t.join()
#     print("%s主线程,done!!!" % (threading.current_thread().name))