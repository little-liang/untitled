'''同一客户端，多次发送，多次接收'''

##缺点，客户端断开，服务端也就挂了，，。。。。

import socket

#3部曲 ，实例 bind listen
server = socket.socket()

server.bind(("localhost", 9999))

server.listen()

print('3部曲 ，实例 bind listen')



while True:
    conn, addr = server.accept()
    print("客户端连接", addr)
#开始堵塞，等待客户端连接
    while True:
        data = conn.recv(512)

        if not data:
            print("客户端断开了")
            break

        print("收到客户端数据", data.decode())
        conn.send(data.upper())

server.close()






