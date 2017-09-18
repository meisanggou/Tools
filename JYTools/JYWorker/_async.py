#! /usr/bin/env python
# coding: utf-8

from time import sleep
from JYTools.StringTool import is_string
from _redis import RedisWorker

__author__ = 'meisanggou'


class AsyncRedisWorker(RedisWorker):

    def __init__(self, conf_path=None, heartbeat_value=None, work_tag=None, log_dir=None, stat_work_tag=None, **kwargs):
        RedisWorker.__init__(self, conf_path=conf_path, heartbeat_value=heartbeat_value, work_tag=work_tag,
                             log_dir=log_dir, **kwargs)
        if is_string(stat_work_tag) is False:
            raise TypeError("stat work tag must be string")
        self.stat_work_tag = stat_work_tag
        self.after_handler_funcs.append(self.after_handler)

    def after_handler(self):
        r_tag = self.current_task.task_report_tag
        self.current_task.task_report_tag = None
        self.push_task(self.current_task.task_key, self.current_task.task_output, work_tag=self.stat_work_tag,
                       report_tag=r_tag, sub_key=self.current_task.task_sub_key)


class AsyncStatRedisWorker(RedisWorker):

    def __init__(self, conf_path=None, heartbeat_value=None, work_tag=None, log_dir=None, **kwargs):
        RedisWorker.__init__(self, conf_path=conf_path, heartbeat_value=heartbeat_value, work_tag=work_tag,
                             log_dir=log_dir, **kwargs)
        self.top_queue = None
        self.before_handler_funcs.append(self.before_handler)

    def before_handler(self):
        if self.current_task == self.top_queue:
            sleep(10)
            self.top_queue = None

    def whether_completed(self, key, params):
        return True

    def handler_task(self, key, params):
        completed = self.whether_completed(key, params)
        if completed is True:
            return
        r_tag = self.current_task.task_report_tag
        self.current_task.task_report_tag = None
        self.push_task(self.current_task.task_key, self.current_task.task_output, report_tag=r_tag,
                       sub_key=self.current_task.task_sub_key)
        if self.top_queue is None:
            self.top_queue = self.current_task

