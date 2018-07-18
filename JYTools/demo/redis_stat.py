#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import RedisStat

__author__ = '鹛桑够'


rs = RedisStat()
print(rs.conf_path)
q = rs.list_queue()
# print(q)
# qd = rs.list_queue_detail("FuncEnrLncPre")
# print(qd)
# w = rs.list_worker()
# print(w)
# wd = rs.list_worker_detail("Plus_4")
# print(wd)
# wq = rs.list_worry_queue()
# print(wq)
# lh = rs.list_heartbeat()
# for item in lh:
#     print(rs.list_heartbeat_detail(item))

task_items = rs.list_task_item("Pipeline", 210)
print(task_items)
