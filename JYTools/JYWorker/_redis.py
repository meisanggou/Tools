#! /usr/bin/env python
# coding: utf-8

import os
import json
from time import sleep
from datetime import datetime
from JYTools import TIME_FORMAT
from JYTools import StringTool
from _config import RedisWorkerConfig, WorkerConfig
from _Worker import Worker
from _Task import WorkerTask

__author__ = 'meisanggou'


class RedisQueue(RedisWorkerConfig, WorkerConfig):
    def __init__(self, conf_path, **kwargs):
        RedisWorkerConfig.__init__(self, conf_path)
        WorkerConfig.__init__(self, conf_path, **kwargs)

    @staticmethod
    def package_task_info(work_tag, key, params, sub_key=None, report_tag=None, is_report=False):
        """
        info format: work_tag[|report_tag],key[|sub_key],args_type,args
        args_type: json
        example: jy_task,key_1,json,{"v":1}
        example: jy_task|ping,key_1|first,json,{"v":1}
        """
        if sub_key is not None:
            key = "%s|%s" % (key, sub_key)
        if report_tag is not None:
            work_tag = "%s|%s" % (work_tag, report_tag)
        v = "%s,%s," % (work_tag, key)
        if isinstance(params, dict):
            if is_report is False:
                v += "json," + json.dumps(params)
            else:
                v += "report," + json.dumps(params)
        else:
            v += "string," + params
        return v

    def push_head(self, key, params, work_tag=None):
        v = self.package_task_info(self.work_tag, key, params)
        self.redis_man.lpush(self.queue_key, v)

    def push_tail(self, key, params, work_tag=None, sub_key=None, report_tag=None):
        v = self.package_task_info(self.work_tag, key, params, sub_key=sub_key, report_tag=report_tag)
        self.redis_man.rpush(self.queue_key, v)

    def push(self, key, params, work_tag=None, sub_key=None, report_tag=None):
        self.push_tail(key, params, work_tag, sub_key=sub_key, report_tag=report_tag)


class RedisWorker(RedisWorkerConfig, Worker):
    """
        expect_params_type
        add in version 0.1.8
    """
    expect_params_type = None

    def __init__(self, conf_path=None, heartbeat_value="0", **kwargs):
        self.conf_path = conf_path
        RedisWorkerConfig.__init__(self, conf_path)
        Worker.__init__(self, conf_path=conf_path, **kwargs)
        self.heartbeat_value = StringTool.decode(heartbeat_value)
        self.redis_man.set(self.heartbeat_key, heartbeat_value)

    def has_heartbeat(self):
        current_value = StringTool.decode(self.redis_man.get(self.heartbeat_key))
        if current_value != self.heartbeat_value:
            self.worker_log("heartbeat is", self.heartbeat_value, "now is", current_value)
            return False
        return True

    def pop_task(self, freq=0):
        try:
            next_task = self.redis_man.blpop(self.queue_key, self.pop_time_out)
        except Exception as e:
            if freq > 5:
                raise e
            freq += 1
            sleep(10)
            return self.pop_task(freq)
        if next_task is not None:
            return next_task[1]
        return next_task

    def pop_task_detail(self, key=None, sub_key=None):
        if key is None:
            key = self.current_task.task_key
        if key is None:
            return None
        detail_key = "%s" % key
        if sub_key is not None:
            detail_key += "_%s" % sub_key
        return self.redis_man.get(detail_key)

    def push_task(self, key, params, work_tag=None, sub_key=None, is_report=False):
        if work_tag is None:
            queue_key = self.queue_key
        else:
            queue_key = self.queue_prefix_key + "_" + work_tag
        task_info = RedisQueue.package_task_info(work_tag, key, params, sub_key=sub_key, is_report=is_report)
        self.redis_man.rpush(queue_key, task_info)

    def push_task_detail(self, task_detail, key=None, sub_key=None):
        if key is None:
            key = self.current_task.task_key
        if key is None:
            return None
        detail_key = "%s" % key
        if sub_key is not None:
            detail_key += "_%s" % sub_key
        self.redis_man.set(detail_key, task_detail)

    def worker_log(self, *args, **kwargs):
        if self.log_dir is None:
            return
        msg = StringTool.join(args, " ")
        level = kwargs.pop("level", "INFO")
        if level != "INFO":
            self.publish_message(msg)
        log_file = os.path.join(self.log_dir, "%s.log" % self.work_tag)
        now_time = datetime.now().strftime(TIME_FORMAT)
        write_a = ["[", self.heartbeat_value]
        if self.worker_index is not None:
            write_a.extend([":", self.worker_index])
        write_a.extend(["] ", now_time, ": ", level, " ", msg, "\n"])
        with open(log_file, "a", 0) as wl:
            s = StringTool.join(write_a, join_str="")
            s = StringTool.encode(s)
            wl.write(s)

    def task_log(self, *args, **kwargs):
        if self.current_task is None:
            return
        msg = StringTool.join(args, " ")
        level = kwargs.pop("level", "INFO")
        if level != "INFO":
            self.publish_message("%s\n%s" % (self.current_task.task_key, msg))
        log_file = os.path.join(self.log_dir, "%s_%s.log" % (self.work_tag, self.current_task.task_key))
        now_time = datetime.now().strftime(TIME_FORMAT)
        write_a = ["[", self.heartbeat_value]
        if self.worker_index is not None:
            write_a.extend([":", self.worker_index])
        write_a.extend(["] ", now_time, ": ", level, " ", msg, "\n"])
        with open(log_file, "a", 0) as wl:
            s = StringTool.join(write_a, join_str="")
            s = StringTool.encode(s)
            wl.write(s)

    def handler_invalid_task(self, task_info, error_info):
        self.worker_log(error_info, level="WARING")

    def parse_task_info(self, task_info):
        task_item = WorkerTask(task_info=task_info)

        partition_task = task_info.split(",", 3)
        if len(partition_task) != 4:
            error_msg = "Invalid task %s, task partition length is not 3" % task_info
            return False, error_msg

        work_tags = partition_task[0].split("|")  # 0 work tag 1 return tag
        if work_tags[0] != self.work_tag:
            error_msg = "Invalid task %s, task not match work tag %s" % (task_info, self.work_tag)
            return False, error_msg
        task_item.set(work_tag=work_tags[0])
        if len(work_tags) > 1:
            task_item.set(task_report_tag=work_tags[1])

        keys = partition_task[1].split("|")
        task_item.set(task_key=keys[0])
        if len(keys) > 1:
            task_item.set(task_sub_key=keys[1])

        if partition_task[2] not in ("string", "json", "report"):
            error_msg = "Invalid task %s, task args type invalid" % task_info
            return False, error_msg
        params = partition_task[3]
        if partition_task[2] in ("json", "report"):
            try:
                params = json.loads(params)
            except ValueError:
                error_msg = "Invalid task %s, task args type and args not uniform" % task_info
                return False, error_msg
        if self.expect_params_type is not None:
            if not isinstance(params, self.expect_params_type):
                return False, "Invalid task, not expect param type"
        task_item.set(task_params=params)
        if partition_task[2] == "report":
            task_item.set(is_report_task=True)
        return True, task_item

    def run(self):
        self.worker_log("Start Run Worker")
        self.worker_log("Worker Conf Path Is ", self.conf_path)
        self.worker_log("Worker Heartbeat Value Is", self.heartbeat_value)
        self.worker_log("Worker Work Tag Is ", self.work_tag)
        self.worker_log("Worker QueHeartbeat Key Is", self.heartbeat_key)
        self.worker_log("Worker Queue Key Is", self.queue_key)

        while True:
            if self.has_heartbeat() is False:
                self.close()
            next_task = self.pop_task()
            if next_task is None:
                continue
            parse_r, task_item = self.parse_task_info(next_task)
            if parse_r is False:
                self.handler_invalid_task(next_task, task_item)
                continue
            self.current_task = task_item
            self.worker_log("Start Execute", self.current_task.task_key)
            self.execute()
            self.worker_log("Completed Task", self.current_task.task_key)


if __name__ == "__main__":
    r_worker = RedisWorker(log_dir="/tmp", heartbeat_value="中文", worker_index=1)
    print(r_worker.log_dir)
    print(r_worker.work_tag)
    r_worker.work()