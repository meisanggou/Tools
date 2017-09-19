#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import AsyncRedisWorker

__author__ = 'meisanggou'


class PlusWorker(AsyncRedisWorker):
    def handler_task(self, key, params):
        print("Enter Plus Worker")
        if "a" not in params:
            self.set_current_task_invalid("Need a")

        if "b" not in params:
            self.set_current_task_invalid("Need b")

        self.task_log("a is ", params["a"])
        self.task_log("b is ", params["b"])
        c = params["a"] + params["b"]
        self.set_output("c", c)
        print("End Plus Task")


p_w = PlusWorker(conf_path="redis_worker.conf", heartbeat_value="FFFFFF", work_tag="Plus",
                 stat_work_tag="STAT")
p_w.work()
