import socket

client = socket.socket()

client.connect(("localhost", 9999))

while True:

    user_input = input(">>:")
    user_input = user_input.strip()

    if len(user_input) == 0 :
        print("你输入的是空，请输入任意字符")
        continue

    client.send(user_input.encode("utf-8"))

    data = client.recv(1014)

    print("收到服务器数据：", data.decode("utf-8"))

client.close()