#! /usr/bin/env python
# coding: utf-8

from uuid import uuid4
from JYTools.JYWorker import RedisWorker, RedisQueue

__author__ = 'meisanggou'

"""
"""


class FirstWorker(RedisWorker):
    def handler_task(self, key, args):
        print(key)
        print(self.current_task.task_sub_key)
        print(self.current_task.task_report_tag)
        key = int(key)
        for i in range(0, 10):
            self.task_log(key + i)


class MSGManager(object):
    publish_message2 = ""

    def publish_message(self, message, key):
        print(message)
        print(key)


r_queue = RedisQueue(conf_path="conf/redis_worker.conf")
for i in range(133, 134):
    r_queue.push(i, {"a": "j"}, sub_key="ddd2", report_tag="One")


r_work = FirstWorker(conf_path="conf/redis_worker.conf", heartbeat_value=uuid4().hex, log_dir="/tmp/", worker_index=2)
msg_man = MSGManager()
r_work.msg_manager = msg_man
# r_work.redirect_stdout = True
r_work.work()
