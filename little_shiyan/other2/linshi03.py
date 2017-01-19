import time


a = time.time()
time.sleep(4)
b = time.time()

c = b - a
d = c/60
print(type(d))
print(round(d, 2))