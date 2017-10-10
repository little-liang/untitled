import threading
import  time

class MyThread(threading.Thread):
    def __init__(self, num):
        super(MyThread,self).__init__()
        self.num = num

    def run(self):
        print("%s" % (self.num))
        time.sleep(2)

if __name__ == '__main__':

    t1 = MyThread(22)
    t2 = MyThread(33)
    t3 = MyThread(44)

    t1.start()
    t2.start()
    t3.start()