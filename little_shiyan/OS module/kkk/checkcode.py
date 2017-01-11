import random

checkcode = ''
for line in range(0,6):
    aa = random.randrange(0,6)
    if aa != line:
        temp = chr(random.randint(65,90))
    else:
        temp = str(random.randint(0,9))
    checkcode += temp
print(checkcode)
