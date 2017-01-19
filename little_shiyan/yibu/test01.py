import socket
import threading

def sock_conn():

    client = socket.socket()

    client.connect(("localhost",8001))
    count = 0
    #一次发送接收100次
    while count < 100:
        #msg = input(">>:").strip()
        #if len(msg) == 0:continue
        client.send( ("hello %s" % count).encode("utf-8"))

        data = client.recv(1024)

        print("[%s]recv from server:" % threading.get_ident(), data.decode()) #结果
        count += 1
    client.close()


##模拟了5个的100次发送接收数据
for i in range(5):
    t = threading.Thread(target=sock_conn)
    t.start()

