'''同一客户端，多次发送，多次接收'''
import socket

client = socket.socket()


client.connect(("localhost", 9999))

while True:
    user_input = input("输入:\n")
    user_input = user_input.strip()
    if len(user_input) == 0:
        continue
    client.send(user_input.encode("utf-8"))

    data = client.recv(1024)
    print("收到服务器信息：", data.decode("utf-8"))

client.close()



