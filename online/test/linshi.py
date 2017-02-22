import sys, time

with open("/tmp/test02.log", 'a') as f:
    f.write("test02")
    f.write(str(sys.argv))
    f.write("\n")


print("i m   test02")

time.sleep(2)

print("sleep done")

for line in range(1, 10):
    print(line)
    time.sleep(0.3)