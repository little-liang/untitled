import multiprocessing
import os, time, random

def sub_process(name):
    print('运行子进程：%s pid is (%s)...' % (name, os.getpid()))
    start = time.time()
    time.sleep(2)
    end = time.time()
    print('子进程 %s 花费 %0.2f 秒.' % (name, (end - start)))

if __name__ == '__main__':
    pool = multiprocessing.Pool(4)


    for line in range(5):
        print("父进程%s开始了 pid is %s" % (line, os.getpid()))
        pool.apply_async(sub_process, args=(line, ))
    pool.close()
    pool.join()