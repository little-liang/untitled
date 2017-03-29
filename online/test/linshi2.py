import time
import paramiko


t = paramiko.Transport('192.168.10.103', 22)
t.connect(username='root', password='Abcd@1234')

sftp = paramiko.SFTPClient.from_transport(t)

localfile = r'tmp.log'
remotefile = r'/tmp/tmp.log'
sftp.put(localfile, remotefile)


