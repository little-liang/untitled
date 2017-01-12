import socket

#客户端两部曲，实例化，连接
client = socket.socket()
client.connect(("localhost", 9999))


#为了客户端可以，多次发送命令
while True:

    #用户输入命令
    user_input = input("输入：").strip()

    #用户输入为空，直接重循环
    if len(user_input) == 0:
        print("你输入为空：请继续：")
        continue

    #客户端发送数据
    client.send(user_input.encode())

    #服务端告诉客户端，接收数据长度
    total_resize = int(client.recv(1024).decode())
    print("服务端要发送过来[%s]长的数据" % (total_resize))

    #客户端发送确认接收命令，
    client.send(b"ok, low b")

    #接收真正的数据

    #初始化接收数据的长度容器，和数据内容容器
    receive_size = 0
    res_data = b''

    ##循环收取数据，直到收取完成
    while receive_size != total_resize:
        data = client.recv(1024)
        receive_size += len(data)
        res_data += data
    else:
        pass

    print("所有的数据已经收完，长度共计", len(res_data))
    # print("数据为：", res_data)

client.close()
