class Role(object):

    #全局变量
    AC = "PPOO"

    #初始化方法，self是为了实例化时用到的
    def __init__(self, name, role, weapon, life_value,):
        self.name = name
        self.role = role
        self.weapon = weapon
        self.life_val = life_value

    #成员方法，调用时用类的变量
    def buy_weapon(self, weapon):
        print("[%s] is buying the [%s]" % (self.name, self.weapon))
        print(self.AC)


p1 = Role("longge", "police", "B11", "100")
t1 = Role("zha ng", "Terrorist", "B10", "100")

p1.buy_weapon("M16")
t1.buy_weapon("AK47")

print("P1:", p1.weapon)
print("T1:", t1.weapon)

##局部重新定义
p1.AC = "LLL"

##全局变量重新定义
Role.AC = "HHH"

print(p1.AC)
print(t1.AC)
