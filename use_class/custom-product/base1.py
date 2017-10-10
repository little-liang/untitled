import time,random
import queue,threading
q = queue.Queue()
def Producer(name):
  count = 1
  while count <=5:
    time.sleep(1)
    q.put(count)
    print('Producer %s has produced %s baozi..' %(name, count))
    count +=1

def Consumer(name):
  count = 1
  while count <=5:
    time.sleep(2)
    if not q.empty():
        data = q.get()
        print(data)
        print('\033[32;1mConsumer %s has eat %s baozi...\033[0m' %(name, data))
    else:
        print("-----no baozi anymore----")
    count += 1
p1 = threading.Thread(target=Producer, args=('A',))
c1 = threading.Thread(target=Consumer, args=('B',))
p1.start()
c1.start()