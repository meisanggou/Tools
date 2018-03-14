#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import ReadWorkerLog

__author__ = '鹛桑够'

rwl = ReadWorkerLog(log_dir="/tmp")
exec_r, logs_list = rwl.read_task_log("Plus", "6669", sub_key="1", sub_key_prefix="", level="INFO")
for log in logs_list:
    print(log)
    print(log[3])
print(len(logs_list))