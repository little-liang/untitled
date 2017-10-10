'''简单的实现socket功能，单次客户端发送，单次接收'''


import socket

#实例化socket对象
server = socket.socket()

#bind一个服务端地址
server.bind(("localhost", 9999))

#开始监听
server.listen()

print("1 实例化 2 bind ip 端口 3 监听 等待客户端连接中。。。")


'''sk.accept() 必会

　　接受连接并返回（conn,address）,其中conn是新的套接字对象，可以用来接收和发送数据。address是连接客户端的地址。

　　接收TCP 客户的连接（阻塞式）等待连接的到来'''

#开始堵塞，等待客户端连接
conn, addr = server.accept()
print("新连接：", addr)


'''sk.recv(bufsize[,flag]) 必会

　　接受套接字的数据。数据以字符串形式返回，bufsize指定最多可以接收的数量。flag提供有关消息的其他信息，通常可以忽略'''

data = conn.recv(1024)
print("收到消息", data.decode())

server.close()