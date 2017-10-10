import time
import threading

def cost_money():
    global total_money
    print("钱的余额:", total_money)

    print("我取走了一元钱！")
    lock.acquire() #修改数据前加锁
    total_money -= 1
    lock.release() #修改后释放

total_money = 100
thread_list = []
lock = threading.Lock() #生成全局锁
for i in range(100):
    t = threading.Thread(target=cost_money)
    t.start()
    thread_list.append(t)


for t in thread_list:
    t.join()

print("最后余额为：", total_money)