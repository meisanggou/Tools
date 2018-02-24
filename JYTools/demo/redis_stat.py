#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import RedisStat

__author__ = '鹛桑够'


rs = RedisStat()
q = rs.list_queue()
print(q)
w = rs.list_worker()
print(w)
wd = rs.list_worker_detail("Plus_2")
print(wd)