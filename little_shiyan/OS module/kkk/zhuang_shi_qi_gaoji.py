##这是正宗得调用方式,带参数的装饰器

#增加的函数登录操作
def login():
    print("请登录----\n","用户名: 密码:")
    print("登录成功")

#装饰器模块
def deco(func):
    def inner(*args, **kwargs):
        login()
        func(*args, **kwargs)
    return inner

#基础代码模块
@deco
def movie(name,sex):
    print("欢迎=>",name,sex,"=<来到电影首页")

@deco
def picture(name,sex,ip):
    print("欢迎=>", name, sex,"IP地址是:",ip,"=<来到电影首页")



#运行开始模块
if __name__=='__main__':
    movie("龙哥","帅哥")
    print("\n\n")
    picture("红姐","美女","7.7.7.7")

