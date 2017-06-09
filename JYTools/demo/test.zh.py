#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import RedisQueue

__author__ = 'meisanggou'

r_queue = RedisQueue("redis_worker.conf", work_tag="Pipeline")

plus_task = {"work_tag": "Plus"}

r_queue.push("abc", {"input_a": 2, "input_b": 3, "task_list": [plus_task]})
