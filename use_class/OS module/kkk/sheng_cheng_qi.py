#取钱,每次只能一百
def take_money(amount):
    while amount > 0:
        amount -= 100
        yield amount
        print("接着取")
    else:
        print("没钱啦")

will_no_money = lambda x:print(x)
atm=take_money(300)
print(type(atm))
print(atm.__next__())
print(atm.__next__())
will_no_money("快没钱了")  ###可以中断一个运行中的函数,做其他动作后在继续执行
print(atm.__next__())
