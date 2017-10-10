import redis

#利用了redis的订阅发布功能
r = redis.Redis(host="192.168.200.188", port=6379)

#打开收音机，
sub = r.pubsub()

#订阅一个频道
sub.subscribe("fm94.8")

#测试一下频道是否通畅，并且一直收数据
while True:
    print(sub.parse_response())