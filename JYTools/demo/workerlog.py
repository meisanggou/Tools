#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import ReadWorkerLog

__author__ = '鹛桑够'

rwl = ReadWorkerLog(log_dir="/mnt/data/Tools")
exec_r, logs_list = rwl.read_task_log("fastq2sort", "27a382a49f5c11e89fccd094663fa8dc", level="INFO")
for log in logs_list:
    print(log)
    print(log[3])
print(len(logs_list))