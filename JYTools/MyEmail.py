#! /usr/bin/env python
# coding: utf-8

from email.header import Header
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib
import thread
import ConfigParser
from datetime import datetime
from My_PC import pc_info

__author__ = 'ZhouHeng'


class MyEmailManager(object):
    def __init__(self, conf_dir):
        self.email_server = "ym.163.com"
        self.sender = "晶云平台"
        conf_path = conf_dir + "email_app.conf"
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

    def encoded(self, s, encoding):
        return s.encode(encoding) if isinstance(s, unicode) else s

    def send_mail(self, to, sub, content):
        try:
            encoding = 'utf-8'
            SMTP = smtplib.SMTP
            smtp = SMTP("smtp.%s" % self.email_server, 25)
            # smtp.set_debuglevel(True)
            user = self.m_user
            smtp.starttls()
            smtp.login(user, self.m_password)
            user = self.encoded(user, encoding)
            user = '{nick_name} <{user}>'.format(nick_name=Header(self.sender, encoding), user=user)
            msg = MIMEMultipart('alternative')
            msg['From'] = user
            msg['To'] = self.encoded(to, encoding)
            msg['Subject'] = Header(self.encoded(sub, encoding), encoding)
            msg.attach(MIMEText(self.encoded(content, encoding), "html", encoding))
            smtp.sendmail(user, to, msg.as_string())
            smtp.quit()
            return True
        except Exception, e:
            error_message = "MyEmailManager send_mail error %s" % str(e.args)
            print(error_message)
            return False

    def send_mail_thread(self, to, sub, content):
        return thread.start_new_thread(self.send_mail, (to, sub, content))
