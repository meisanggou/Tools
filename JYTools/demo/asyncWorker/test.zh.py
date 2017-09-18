#! /usr/bin/env python
# coding: utf-8

from datetime import datetime
from JYTools.JYWorker import RedisQueue

__author__ = 'meisanggou'

r_queue = RedisQueue("redis_worker.conf", work_tag="Pipeline")
print(r_queue.queue_key)

plus_task = {"work_tag": "Plus", "input_a": "&0a", "input_b": "&0b"}

pipeline_detail5 = {"task_list": [plus_task], "input_a": 5, "input_b": 7}
r_queue.push(datetime.now().strftime("%Y%m%d%H%M"), pipeline_detail5)