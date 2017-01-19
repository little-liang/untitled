'''这个程序描述消费者与生产者，生产包子，顾客吃包子，缓冲区就是，蒸包子的容器'''
'''基于队列》》》》》先进先出'''
import queue
import threading
import time

class Product_thread_class(threading.Thread):
    def __init__(self, product_num):
        super(Product_thread_class, self).__init__()
        self.product_num = product_num

    def run(self):
        count = 1
        while count <= 5:
            print("生产者生产第%s包子...\n" % (count))
            q.put(count)
            count += 1
            time.sleep(0.5)

class Custom_thread_class(threading.Thread):
    def __init__(self, custom_name):
        super(Custom_thread_class, self).__init__()
        self.custom_name = custom_name

    def run(self):
        time.sleep(1)
        if not q.empty():
            print("%s消费者吃了一个包子,编号是%s" % (self.custom_name, q.get()))
        else:
            print("------目前容器中没有包子----")



if __name__ == '__main__':
    q = queue.Queue()
    product1 = Product_thread_class(5)
    product1.start()
    product1.join()  # 这里认为的将生产者速度变慢，让生产一个一个生产 ##也可以让消费者变慢消费
                        #总之这里很不好，需要改进，等用到时，再说吧

    custom_list = ('zhao', 'qian', 'sun', 'li', 'zhou')
    for name in custom_list:
        custom1 = Custom_thread_class(name)
        custom1.start()
