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
    """
        conf_path_environ_key
        add in version 0.1.25
    """
    conf_path_environ_key = "REDIS_WORKER_CONF_PATH"

    def __init__(self, conf_path, **kwargs):
        self.conf_path = conf_path
        if os.path.exists(self.conf_path) is False:
            print("Conf Path Not Exist ", self.conf_path)
            print("Read os environ :", self.conf_path_environ_key, " ")
            env_conf_path = os.environ.get(self.conf_path_environ_key)
            print("os environ ", self.conf_path_environ_key, " is ", env_conf_path)
            if env_conf_path is not None:
                if os.path.exists(env_conf_path) is True:
                    self.conf_path = env_conf_path
                    print("Use ", env_conf_path, " As conf path")
                else:
                    print("Path ", env_conf_path, " Not Exist")
        RedisWorkerConfig.__init__(self, self.conf_path)
        WorkerConfig.__init__(self, self.conf_path, **kwargs)

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


class RedisData(object):
    BOOL_VALUE = [False, True]

    @staticmethod
    def package_data(data):
        if data is None:
            return ""
        if isinstance(data, dict):
            return "d_" + json.dumps(data)
        if isinstance(data, list):
            return "l_" + json.dumps(data)
        if isinstance(data, bool):
            return "b_%s" % RedisData.BOOL_VALUE.index(data)
        if isinstance(data, int):
            return "i_%s" % data
        if isinstance(data, float):
            return "f_%s" % data
        else:
            return "s_%s" % data

    @staticmethod
    def unpack_data(p_data):
        if isinstance(p_data, (unicode, str)) is False:
            return p_data
        sp_data = p_data.split("_", 1)
        if len(sp_data) != 2:
            return p_data
        sign = sp_data[0]
        if sign == "s":
            return sp_data[1]
        if sign == "d":
            return json.loads(sp_data[1])
        elif sign == "l":
            return json.loads(sp_data[1])
        elif sign == "i":
            return int(sp_data[1])
        elif sign == "f":
            return float(sp_data[1])
        elif sign == "b":
            return RedisData.BOOL_VALUE[int(sp_data[1])]
        return p_data


class RedisWorker(RedisWorkerConfig, Worker):
    """
        expect_params_type
        add in version 0.1.8
    """
    expect_params_type = None
    conf_path_environ_key = "REDIS_WORKER_CONF_PATH"

    def __init__(self, conf_path=None, heartbeat_value="0", **kwargs):
        self.conf_path = conf_path
        if os.path.exists(self.conf_path) is False:
            print("Conf Path Not Exist ", self.conf_path)
            print("Read os environ :", self.conf_path_environ_key, " ")
            env_conf_path = os.environ.get(self.conf_path_environ_key)
            print("os environ ", self.conf_path_environ_key, " is ", env_conf_path)
            if env_conf_path is not None:
                if os.path.exists(env_conf_path) is True:
                    self.conf_path = env_conf_path
                    print("Use ", env_conf_path, " As conf path")
                else:
                    print("Path ", env_conf_path, " Not Exist")
        RedisWorkerConfig.__init__(self, self.conf_path)
        Worker.__init__(self, conf_path=self.conf_path, **kwargs)
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

    def push_task(self, key, params, work_tag=None, sub_key=None, report_tag=None, is_report=False):
        if work_tag is None:
            queue_key = self.queue_key
        else:
            queue_key = self.queue_prefix_key + "_" + work_tag
        task_info = RedisQueue.package_task_info(work_tag, key, params, sub_key=sub_key, report_tag=report_tag,
                                                 is_report=is_report)
        self.redis_man.rpush(queue_key, task_info)

    def _task_item_key(self, item_index=None, key=None, sub_key=None):
        if key is None:
            key = self.current_task.task_key
        if key is None:
            return None
        item_key = "%s_%s" % (self.queue_key, key)
        if sub_key is None:
            sub_key = self.current_task.task_sub_key
        if sub_key is not None:
            item_key += "_%s" % sub_key
        if item_index is not None:
            item_key += "_%s" % item_index
        return item_key

    def set_task_item(self, item_index, hash_key, hash_value, key=None, sub_key=None, nx=False):
        item_key = self._task_item_key(item_index, key, sub_key)
        if nx is True:
            return self.redis_man.hsetnx(item_key, hash_key, RedisData.package_data(hash_value))
        self.redis_man.hset(item_key, hash_key, RedisData.package_data(hash_value))

    def get_task_item(self, item_index, hash_key=None, key=None, sub_key=None):
        item_key = self._task_item_key(item_index, key, sub_key)
        if hash_key is None:
            item = self.redis_man.hgetall(item_key)
            for key in item.keys():
                item[key] = RedisData.unpack_data(item[key])
            return item
        return RedisData.unpack_data(self.redis_man.hget(item_key, hash_key))

    def del_task_item(self, item_index, key=None, sub_key=None):
        item_key = self._task_item_key(item_index, key, sub_key)
        return self.redis_man.delete(item_key)

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
        if self.current_task is None or self.current_task.task_key is None:
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
        if self.current_task.task_sub_key is not None:
            write_a.extend(["][", self.current_task.task_sub_key])
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

