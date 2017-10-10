'''互斥锁有多把钥匙,多个人可以同时看'''
'''这里重要应用是限制连接数据的连接数量'''
#目前不会用，不太懂，不过依旧实现了线程多并发执行

import threading
import time


def sub_thread():
    #互斥锁开始锁上
    semaphore.acquire()
    print('\t子线程%s正在运行...' % (threading.current_thread().name))
    time.sleep(1)
    print("\t子线程%s完成部件" % (threading.current_thread().name))
    time.sleep(1)
    print('\t子线程%s已经完成工作!\n' % (threading.current_thread().name))

    semaphore.release()


if __name__ == '__main__':
    print("%s主线程正在运行...\n" % (threading.current_thread().name))
    semaphore = threading.BoundedSemaphore(5) #最多允许5个线程同时运行
    for i in range(5):
        t = threading.Thread(target=sub_thread)
        t.start()


    #这里的意思是线程数=1时，代表肯定是最后一个线程，最后一个子线程完成，那么主线程就完成了
    while threading.active_count() != 1:
        pass
    else:
        print('----all threads done---')

    print("%s主线程,done!!!" % (threading.current_thread().name))