#! /usr/bin/env python
# coding: utf-8

from JYTools.StringTool import is_string
from _redis import RedisWorker

__author__ = 'meisanggou'


class AsyncRedisWorker(RedisWorker):

    def __init__(self, conf_path=None, heartbeat_value=None, work_tag=None, log_dir=None, qstat=None, **kwargs):
        RedisWorker.__init__(self, conf_path=conf_path, heartbeat_value=heartbeat_value, work_tag=work_tag,
                             log_dir=log_dir, **kwargs)
        if is_string(qstat) is False:
            raise TypeError("qstat must be string")
        self.stat_tag = qstat

    def after_handler(self):
        r_tag = self.current_task.task_report_tag
        self.current_task.task_report_tag = None
        self.push_task(self.current_task.task_key, work_tag=self.stat_tag, report_tag=r_tag,
                       sub_key=self.current_task.task_sub_key)