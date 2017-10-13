import threading
import time
import queue


def sub_thread():
    aa = q.get()
    time.sleep(3)
    print('\t子线程%s已经完成工作! %s' % ((threading.current_thread().name), aa))




if __name__ == '__main__':

    q = queue.Queue()
    for line in range(10):
        q.put(line)

    #定义线程池
    thread_list = []

    #主进程开始

    print("%s主线程正在运行..." % (threading.current_thread().name))

    while True:

        if not q.empty():
            for i in range(3):
                t = threading.Thread(target=sub_thread)
                t.start()
                thread_list.append(t)

            ##等待子进程或者线程结束后再继续往下运行（这时候只要线程 池中有线程，就接着等。。）
            for t in thread_list:
                t.join()


        time.sleep(2)
        print('线程小的们都死了, 下一波开始')
