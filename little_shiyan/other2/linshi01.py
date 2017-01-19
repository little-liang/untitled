
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr


msg = MIMEText('邮件内容这个是邮件内容', 'plain', 'utf-8')
msg['From'] = formataddr(["武沛齐名字随便，但必须有",'liangyanlong@hydsoft.com'])
msg['To'] = formataddr(["走人名字随便，但必须有",'liangyanlong@hydsoft.com'])
msg['Subject'] = "主题，邮件的主题"

server = smtplib.SMTP("smtp.hydsoft.com", 25)
server.login("liangyanlong@hydsoft.com", "Abcd1234")
server.sendmail('liangyanlong@hydsoft.com', ['liangyanlong@hydsoft.com',], msg.as_string())
server.quit()