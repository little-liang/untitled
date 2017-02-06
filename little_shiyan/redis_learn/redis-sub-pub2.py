import redis

#利用了redis的订阅发布功能
r = redis.Redis(host="192.168.200.188", port=6379)

r.publish("fm94.8", "MSG1")