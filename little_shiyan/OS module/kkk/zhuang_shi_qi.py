#原理,简单
# def login(func):
#     print("用户名密码输入成功!")
#     return func
# @login
# def tv():
#     print("欢迎来到TV网页")
# tv()

##这是正宗得调用方式

#登录模块
def login(mem_addr):
    def inner(pro):
        print("请登录----")
        print("登录成功")
        return pro
    return inner

#基础代码模块
@login
def movie(name):
    print("欢迎来到电影首页")

#运行开始模块
if __name__=='__main__':
    aaa=movie("龙哥")
    print(aaa)




