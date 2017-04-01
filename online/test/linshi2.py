import time, hashlib


def get_token(username, token_id):
    timestamp = int(time.time())
    md5_format_str = "%s %s %s" % (username, timestamp, token_id)
    md5_format_str = md5_format_str.encode()
    obj = hashlib.md5(md5_format_str)

    print("加的盐是", "token format:[%s]" % (md5_format_str.decode()))


    print("传输的md5值", "token :[%s]" % (obj.hexdigest()))

    #这里MD5太长 直截取一小段
    return obj.hexdigest()[10:17], timestamp

if __name__ == '__main__':
    print(get_token('alex', 'test'))
