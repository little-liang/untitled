
'''本程序实现的是远程登陆（其实没有做），运行命令后，返回所有结果，其实也可以用cat 命令来传输文件'''
''' 知识点，主要有socket的粘包,服务器用sendall来解决，接收长度的固定大小问题，在客户端用循环收取，来解决'''
import socket
import subprocess

#server socket 3部曲
server = socket.socket()
server.bind(("localhost", 9999))
server.listen()
print("等待客户端连接...")


#实例化套接字，开始堵塞
conn, addr = server.accept()
print("客户端开始连接...", addr)

#可以输入多次命令（客户端使用）
while True:

    #接收客户端数据（功能上，客户端开始发送一个shell命令）
    data = conn.recv(1024)

    #这里发送的数据如果是0长度，就退出，也可以再来一个循环，包起来
    if len(data) == 0:
        break
    print("\t收到客户端数据：", data.decode())

    #用客户端发送的命令开始调用subprocess执行，把执行结果放入res变量中
    res = subprocess.Popen(data.decode(), shell=False, stdout=subprocess.PIPE).stdout.read()

    #服务端发送说，客户端小哥，你接收的数据长度是多少，你确认接受么，
    conn.send(str(len(res)).encode())
    print("即将发送%s长度给客户端，等待客户端ACK确认" % (str(len(res))))

    #客户端发送确认应答，我服务端接收
    client_ACK = conn.recv(1024)
    print("\t客户端应答：", client_ACK)

    #开始发送所有数据，这里的长度测试超过，可接收长度
    print("发送全部数据中...")
    conn.sendall(res)

#关闭连接
server.close()

