#! /usr/bin/env python
# coding: utf-8

import os
from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import thread
import ConfigParser

__author__ = 'ZhouHeng'


class EmailManager(object):
    def __init__(self, conf_dir):
        self.email_server = "ym.163.com"
        self.sender = "晶云平台"
        conf_path = os.path.join(conf_dir, "email_app.conf")
        if os.path.exists(conf_path) is False:
            conf_path = os.path.join(conf_dir, "email.conf")
        self._int_app(conf_path)

    def _int_app(self, conf_path):
        config = ConfigParser.ConfigParser()
        config.read(conf_path)
        section = "admin"
        try:
            self.m_user = config.get(section, "email")
            self.m_password = config.get(section, "password")
        except ConfigParser.Error:
            self.m_user = ""
            self.m_password = ""

    def encoded(self, s, encoding="utf-8"):
        return s.encode(encoding) if isinstance(s, unicode) else s

    def send_mail(self, to, sub, content):
        try:
            encoding = 'utf-8'
            SMTP = smtplib.SMTP_SSL
            smtp = SMTP("smtp.%s" % self.email_server, 465)
            # smtp.set_debuglevel(True)
            user = self.m_user
            smtp.login(user, self.m_password)
            user = self.encoded(user, encoding)
            user = '{nick_name} <{user}>'.format(nick_name=Header(self.sender, encoding), user=user)
            msg = MIMEMultipart('alternative')
            msg['From'] = user
            msg['To'] = self.encoded(to, encoding)
            msg['Subject'] = Header(self.encoded(sub, encoding), encoding)
            msg.attach(MIMEText(self.encoded(content, encoding), "html", encoding))
            smtp.sendmail(self.m_user, to, msg.as_string())
            smtp.quit()
            return True
        except Exception, e:
            error_message = "MyEmailManager send_mail error %s" % str(e)
            print(error_message)
            return False

    def send_mail_thread(self, to, sub, content):
        return thread.start_new_thread(self.send_mail, (to, sub, content))

if __name__ == "__main__":
    email_man = EmailManager(conf_dir="/mnt/data/JINGD/conf")
    email_man.send_mail("zhouheng@gene.ac", "TEST", "TEST SSL SEND")