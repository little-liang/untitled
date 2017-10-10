def f1(arg1,arg2,stop):
    if arg1 == 0:
        b.append(arg1)
        b.append(arg2)
    arg3 = arg1+arg2
    b.append(arg3)
    if arg3 < stop:
        f1(arg2,arg3,stop)
b=[]
f1(0,1,55)
print(b)