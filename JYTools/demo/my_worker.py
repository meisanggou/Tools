#! /usr/bin/env python
# coding: utf-8

from uuid import uuid4
from JYTools.JYWorker import RedisWorker, RedisQueue

__author__ = 'meisanggou'

"""
"""


class FirstWorker(RedisWorker):
    def handler_task(self, key, args):
        key = int(key)
        for i in range(0, 10):
            self.task_log(key + i)


r_work = FirstWorker(conf_path="conf/redis_worker.conf", heartbeat_value=uuid4().hex, log_dir="/tmp/", work_tag="")
r_work.run()

"""
"""
r_queue = RedisQueue(conf_path="conf/redis_worker.conf")
for i in range(133, 144):
    r_queue.push(i, {"a": "j"})
