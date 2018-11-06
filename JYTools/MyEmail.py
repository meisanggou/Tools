#! /usr/bin/env python
# coding: utf-8

import os
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
from multiprocessing.pool import ThreadPool
import ConfigParser

__author__ = 'ZhouHeng'


class EmailManager(object):
    def __init__(self, conf_dir, thread_num=5):
        self.email_server = "ym.163.com"
        self.sender = "晶云平台"
        conf_path = os.path.join(conf_dir, "email_app.conf")
        if os.path.exists(conf_path) is False:
            conf_path = os.path.join(conf_dir, "email.conf")
        self.m_user = "admin@localhost"
        self.m_password = ""
        self._int_app(conf_path)
        self.encoding = 'utf-8'
        self.send_user = '%s <%s>' % (Header(self.sender, self.encoding), self.encoded(self.m_user))
        self.t_pool = ThreadPool(thread_num)

    def _int_app(self, conf_path):
        config = ConfigParser.ConfigParser()
        config.read(conf_path)
        section = "admin"
        try:
            self.m_user = config.get(section, "email")
            self.m_password = config.get(section, "password")
            self._send = self._remote_send
        except ConfigParser.Error:
            self._send = self._local_send

    def encoded(self, s, encoding="utf-8"):
        return s.encode(encoding) if isinstance(s, unicode) else s

    def _remote_send(self, to, sub, msg):
        try:
            SMTP = smtplib.SMTP_SSL
            smtp = SMTP("smtp.%s" % self.email_server, 465)
            # smtp.set_debuglevel(True)
            smtp.login(self.m_user, self.m_password)

            msg['From'] = self.send_user
            msg['To'] = self.encoded(to, self.encoding)
            msg['Subject'] = Header(self.encoded(sub, self.encoding), self.encoding)

            smtp.sendmail(self.m_user, to, msg.as_string())
            smtp.quit()
            return True
        except Exception, e:
            error_message = "MyEmailManager send_mail error %s" % str(e)
            print(error_message)
            return False

    def _local_send(self, to, sub, msg):
        try:
            smtp = smtplib.SMTP("localhost")
            # smtp.set_debuglevel(True)
            msg['From'] = self.send_user
            msg['To'] = self.encoded(to, self.encoding)
            msg['Subject'] = Header(self.encoded(sub, self.encoding), self.encoding)

            smtp.sendmail(self.m_user, to, msg.as_string())
            smtp.quit()
            return True
        except Exception, e:
            error_message = "MyEmailManager send_mail error %s" % str(e)
            print(error_message)
            return False

    def send_mail(self, to, sub, content):
        return self.send_attachment(to, sub, content, [])

    def send_attachment(self, to, sub, content, attachments):
        msg = MIMEMultipart('alternative')
        msg.attach(MIMEText(self.encoded(content, self.encoding), "html", self.encoding))
        if isinstance(attachments, list):
            real_attachments = set(map(lambda x: os.path.realpath(x), attachments))
            for attach in real_attachments:
                att_item = MIMEText(open(attach, "rb").read(), 'base64', self.encoding)
                att_item["Content-Type"] = "application/octet-stream"
                att_item["Content-Disposition"] = "attachment; filename=%s" % os.path.basename(attach)
                msg.attach(att_item)
        return self._send(to, sub, msg)

    def send_mail_thread(self, to, sub, content):
        return self.t_pool.apply_async(self.send_mail, (to, sub, content))

    def send_attachment_thread(self, to, sub, content, attachments):
        return self.t_pool.apply_async(self.send_attachment, (to, sub, content, attachments))

if __name__ == "__main__":
    email_man = EmailManager(conf_dir="/mnt/data/JINGD/conf")
    # email_man.send_mail("zhouheng@gene.ac", "Final TEST", "TEST NEW SEND. Only send content")
    # email_man.send_attachment("zhou5315938@163.com", "Final Test", "TEST send Fom local", ["/home/msg/a.txt", "/home/msg/a.txt", "/home/msg/a.txt"])
    # email_man.send_attachment_thread("zhouheng@gene.ac", "Final Test", "TEST send Fom local", ["/home/msg/a.txt", "/home/msg/a.txt", "/home/msg/a.txt"])
    email_man.send_mail_thread("zhouheng@gene.ac", "邮箱验证码", open("/home/msg/tmpmail_21.html").read())
    email_man.t_pool.close()
    email_man.t_pool.join()
