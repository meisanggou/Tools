#! /usr/bin/env python
# coding: utf-8

import os
from JYTools.JYWorker import AsyncStatRedisWorker

__author__ = 'meisanggou'


class T(AsyncStatRedisWorker):

    def whether_completed(self, key, params):
        print(params)
        path = os.path.join(".", "%s.txt" % params["c"])
        print(path)
        print(os.path.abspath(path))
        print(os.path.exists(path))
        if os.path.exists(path):
            return True
        return False


p_w = T(conf_path="redis_worker.conf", heartbeat_value="FFFFFF", log_dir="/tmp", work_tag="STAT")
p_w.work()