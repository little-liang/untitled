import threading
import time, random
#此程序只能强制杀死
#这里门的运行程序线程是一直存在的，门有开有毕，员工必须知道门的状态，才能解锁或者不解锁。
#因为人可以在前一个同事进去后，跟进去，不用刷门卡
#这两个线程必须进行交互，通信
class staff_swipe_card_class(threading.Thread):
    #初始化类属性
    def __init__(self, name):
        #新式类继承
        super(staff_swipe_card_class, self).__init__()
        self.name = name

    #重新run方法
    def run(self):
        print("%s 来了，查看门是否开着..." % (self.name))
        if door_status_event.is_set():  ##这里就是线程之间的通信了，员工线程访问了门线程
            print("%s 发现门已经开了，直接进去了...\n" % (self.name))

        else:
            print("%s 发现门没开， 掏出他的27K黄金卡，并默念芝麻开门开锁..." % (self.name))
            #把门线程中的门状态设置为开
            door_status_event.set()
            print("咔，门开了，%s进去了\n" %(self.name))

        time.sleep(2)


class door_calss(threading.Thread):
    def __init__(self):
        super(door_calss, self).__init__()

    #门自动关闭程序，超过3秒自动关闭
    def run(self):
        #门开的时间计数
        door_open_time_counter = 0
        while True:
            if door_status_event.is_set():
                # print("我是门！我开了")
                door_open_time_counter += 1
                # print(door_open_time_counter)
            else:
                # print("我是门！我关了")
                door_open_time_counter = 0 #清空计时器
                door_status_event.wait()

            if door_open_time_counter > 3:#门开了已经3s了,该关了
                door_status_event.clear()
            time.sleep(0.5)








#主程序
if __name__ == '__main__':
    #这里得设置一个线程的event(事件)，用于通信,默认为False，这里就是门锁的状态
    door_status_event = threading.Event()

    door1 = door_calss()
    door1.start()


    # #先模仿3个班来上班
    # s1 = staff_swipe_card_class("zhao")
    # s1.start()
    #
    # time.sleep(1)
    # s2 = staff_swipe_card_class("qian")
    # s2.start()
    #
    # time.sleep(4)
    # s3 = staff_swipe_card_class("sun")
    # s3.start()

    ##这里模仿5个人进来刷卡上班。
    staff_list = ['zhao', 'qian', 'sun', 'li', 'zhou', 'wu']
    # staff_list = ['zhao', 'qian']
    for staff in staff_list:
        s = staff_swipe_card_class(staff)
        #模拟员工来的时间不一样
        time.sleep(random.randrange(4))
        time.sleep(0.5)
        s.start()

