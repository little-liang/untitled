import socket, os

server = socket.socket()

server.bind(("localhost", 9999))

server.listen()

print("ssh 进程已经开启，等待连接中。。。")

conn, addr = server.accept()
print("客户端的新连接", addr)

while True:

    data = conn.recv(1024)

    print("收到客户端数据：", data)

    print(data.decode())

    res = os.popen(data.decode("utf-8")).read()

    print(len(res))
    conn.send(res.encode("utf-8"))

server.close()


