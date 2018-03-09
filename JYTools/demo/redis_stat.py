#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import RedisStat

__author__ = '鹛桑够'


rs = RedisStat()
q = rs.list_queue()
print(q)
qd = rs.list_queue_detail("Plus")
print(qd)
w = rs.list_worker()
print(w)
wd = rs.list_worker_detail("Plus")
print(wd)

lh = rs.list_heartbeat()
for item in lh:
    print(rs.list_heartbeat_detail(item))
