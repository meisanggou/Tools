#! /usr/bin/env python
# coding: utf-8

import os
from time import sleep
from ._redis import RedisWorker

__author__ = '鹛桑够'


class UploadLogWorker(RedisWorker):

    expect_params_type = dict

    def upload_log(self, key, log_path, timestamp):
        return True

    def handler_task_exception(self, e):
        self.push_task(self.current_task.task_key, self.current_task.task_params)

    def handle_task(self, key, params):
        log_path = params["log_path"]
        timestamp = params["timestamp"]
        if os.path.isfile(log_path) is False:
            self.set_current_task_invalid(log_path, " Not A File.")
        upload_r = self.upload_log(key, log_path, timestamp)
        if upload_r is True:
            self.task_log("Upload ", log_path, " Success")
            l = self.stat_man.remove_queue_task(self.work_tag, key)
            sleep(2)
            return
        self.task_log("Upload ", log_path, " Fail")
        self.push_task(key, params)
