# -*- coding: utf-8 -*-
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart
try:
    from my_secrets import USER
except Exception as e:
    USER = {
        "email": "xxxx@qq.com",  # 邮箱登录账号
        "password": "xxxx" # 发送人邮箱的授权码
    }

# 设置SMTP服务器以及登录信息
SERVER = {
    'host': "smtp.qq.com",
    'port': 465
}


class PersonMail(object):
    def __init__(self, receivers, sender=USER["email"]):
        self.From = sender
        self.To = receivers
        self.msg = ''


    def _write(self, subject, content):
        self.msg['From'] = Header(self.From)
        self.msg['To'] = Header(str(";".join(self.To)))
        self.msg['Subject'] = Header(subject)

    def write_msg(self, subject, content):
        # 三个参数：第一个为文本内容，第二个 plain 设置文本格式，第三个 utf-8 设置编码
        self.msg = MIMEText(content, 'plain', 'utf-8')
        self._write(subject, content)

    def write_file(self, path):
        self.msg = MIMEMultipart()
        self.msg.attach(MIMEText(open(path).read()))

    def send_email(self):
        try:
            smtp_client = smtplib.SMTP_SSL(SERVER["host"], SERVER["port"])
            smtp_client.login(USER["email"], USER["password"])
            smtp_client.sendmail(self.From, self.To, self.msg.as_string())
            smtp_client.quit()
            return 1
        except smtplib.SMTPException as e:
            print("error", e)
            return 0

def send_qq_mail(msg):
    receivers = [USER["email"]]
    mail = PersonMail(receivers)
    mail.write_msg("You've got new infomation from house spider", msg)
