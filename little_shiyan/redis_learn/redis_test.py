import redis

#实例化一个连接redis实例
r = redis.Redis(host="192.168.200.188", port=6379, )

#显示所有的key
# print(r.keys())


#设置key-value,ex=3超时时间是3秒,
# r.set('NAME', 'longge', ex=3)
# print(r.get('NAME'))

#设置列表
# r.lpush('NUM_LIST', 1, 2, 3, 4, 5)
#读出列表，二进制格式
# print(r.lrange('NUM_LIST', 0, -1))

#读出，utf8格式
for line in r.lrange('NUM_LIST', 0, -1):
    print(line.decode())