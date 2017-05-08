#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import RedisWorker, RedisQueue

__author__ = 'meisanggou'

"""
"""
# r_work = RedisWorker(conf_path="conf/redis_worker.conf", heartbeat_value="first", log_dir="/tmp/")
# r_work.run()

"""
"""
r_queue = RedisQueue(conf_path="conf/redis_worker.conf")
r_queue.push("28", {"a": "j"})
