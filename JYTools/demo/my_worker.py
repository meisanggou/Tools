#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import RedisWorker, RedisQueue

__author__ = 'meisanggou'

"""
"""
# r_work = RedisWorker(conf_path="conf/redis_worker.conf", heartbeat_value="second", log_dir="/tmp/")
# r_work.run()

"""
"""
r_queue = RedisQueue(conf_path="conf/redis_worker.conf")
for i in range(133, 144):
    r_queue.push(i, {"a": "j"})
