#! /usr/bin/env python
# coding: utf-8

from ._redis import RedisWorker

__author__ = '鹛桑够'


class UploadLogWorker(RedisWorker):

    expect_params_type = dict

    @staticmethod
    def upload_log(key, log_path, timestamp):
        return True

    def handle_task(self, key, params):
        log_path = params["log_path"]
        timestamp = params["timestamp"]
        upload_r = self.upload_log(key, log_path, timestamp)
        if upload_r is True:
            self.stat_man.remove_queue_task(self.work_tag, key)
            return
        self.push_task(key, params)
