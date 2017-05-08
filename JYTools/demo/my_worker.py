#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import RedisWorker, RedisQueue
import threading

__author__ = 'meisanggou'

"""
"""
r_work = RedisWorker(conf_path="conf/redis_worker.conf", heartbeat_value="first")
threading.Thread(target=r_work.run).start()

"""
"""
r_queue = RedisQueue(conf_path="conf/redis_worker.conf")
r_queue.push("23,23")
