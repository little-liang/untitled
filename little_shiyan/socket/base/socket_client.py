"""单次发送 单次接收"""

import socket

client = socket.socket()

client.connect(("localhost", 9999))

client.send(b"kkkk")

client.close()
