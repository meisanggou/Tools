#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import ReadWorkerLog

__author__ = '鹛桑够'

rwl = ReadWorkerLog(log_dir="/tmp")
exec_r, logs_list = rwl.read_task_log("Pipeline", "abcde", level="ERROR")
for log in logs_list:
    print(log)
print(len(logs_list))