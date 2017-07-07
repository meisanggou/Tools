#! /usr/bin/env python
# coding: utf-8

import uuid
from time import sleep
from JYTools.JYWorker import RedisWorker

__author__ = 'meisanggou'


class PlusWorker(RedisWorker):
    def handler_task(self, key, params):
        print("Enter Plus Worker")
        if "a" not in params:
            self.set_current_task_invalid("Need a")

        if "b" not in params:
            self.set_current_task_invalid("Need b")

        # if "b1" not in params:
        #     self.set_current_task_invalid("Need b1")

        self.task_log("a is ", params["a"])
        self.task_log("b is ", params["b"])
        sleep(1)
        c = params["a"] + params["b"]
        self.set_output("c", c)
        print("End Plus Task")


p_w = PlusWorker(conf_path="redis_worker.conf", heartbeat_value="FFFFFF", log_dir="/tmp", work_tag="Plus")
p_w.work()
