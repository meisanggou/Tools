#! /usr/bin/env python
# coding: utf-8

import uuid
import json
from time import sleep, time
from JYTools.JYWorker import RedisWorker

__author__ = 'meisanggou'


class ResultWorker(RedisWorker):
    def handle_report_task(self):
        print(self.current_task.task_key)
        task_params = self.current_task.task_params
        print(task_params)
        # print(task_params)
        print("------------------------task errors start---------------------------------")
        for error in task_params.task_errors:
            print(error)
        print("------------------------task errors end---------------------------------")
        print(task_params["task_status"])
        print(task_params["task_message"])
        start_time = task_params["start_time"]
        end_time = task_params["end_time"]
        finished_time = time()
        print("start time ", start_time)
        print("end time ", end_time)
        print("use ", end_time - start_time)
        print(task_params["task_output"])
        print(json.dumps(task_params["sub_task_detail"]))


p_w = ResultWorker(conf_path="redis_worker.conf", log_dir="/tmp", work_tag="Result")
p_w.work()
