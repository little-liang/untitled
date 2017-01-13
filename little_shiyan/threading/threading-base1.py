import threading
import time

def sayhi(name):
    print("hello!", name)
    time.sleep(3)

if __name__ == '__main__':
    name = "longge"
    t1 = threading.Thread(target=sayhi, args=(name,))

    name = "zhang"
    t2 = threading.Thread(target=sayhi, args=(name,))

    t1.start()
    print("thread name is ", t1.getName())

    t2.start()
    print("thread name is ", t2.getName())
