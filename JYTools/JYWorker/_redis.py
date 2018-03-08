#! /usr/bin/env python
# coding: utf-8
import os
import json
import threading
import re
import six
from time import sleep, time
from datetime import datetime
from redis import RedisError
from JYTools import TIME_FORMAT
from JYTools import StringTool
from JYTools.StringTool import is_string
from ._config import RedisWorkerConfig, WorkerConfig
from ._Worker import Worker
from ._Task import WorkerTask, WorkerTaskParams
from ._exception import InvalidTaskKey, InvalidWorkerTag

__author__ = '鹛桑够'


class RedisQueueData(object):
    """
    add in version 0.9.8
    """

    @staticmethod
    def parse_task_info(task_info, work_tag=None, expect_params_type=None):
        task_item = WorkerTask(task_info=task_info)

        partition_task = task_info.split(",", 3)
        if len(partition_task) != 4:
            error_msg = "Invalid task %s, task partition length is not 3" % task_info
            return False, error_msg

        work_tags = partition_task[0].split("|")  # 0 work tag 1 return tag
        if work_tag is not None and work_tags[0] != work_tag:
            error_msg = "Invalid task %s, task not match work tag %s" % (task_info, work_tag)
            return False, error_msg
        task_item.set(work_tag=work_tags[0])
        if len(work_tags) > 1:
            task_item.set(task_report_tag=work_tags[1])

        keys = partition_task[1].split("|")
        if len(keys[0]) <= 0:
            return True, None
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
        if partition_task[2] == "report":
            task_item.set(is_report_task=True)
            task_item.set(task_params=WorkerTask(**params))
        else:
            if expect_params_type is not None:
                if not isinstance(params, expect_params_type):
                    return False, "Invalid task, not expect param type"
            if expect_params_type == dict:
                task_item.set(task_params=WorkerTaskParams(**params))
            else:
                task_item.set(task_params=params)
        return True, task_item


class RedisQueue(RedisWorkerConfig, WorkerConfig):
    """
        conf_path_environ_key
        add in version 0.1.25
    """
    conf_path_environ_key = "REDIS_WORKER_CONF_PATH"

    def __init__(self, conf_path=None, work_tag=None, redis_host=None, redis_password=None, redis_port=None,
                 redis_db=None, section_name="Redis", **kwargs):
        self.conf_path = conf_path
        if self.conf_path is None or os.path.exists(self.conf_path) is False:
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
        RedisWorkerConfig.__init__(self, self.conf_path, redis_host=redis_host, redis_password=redis_password,
                                   redis_port=redis_port, redis_db=redis_db, section_name=section_name)
        WorkerConfig.__init__(self, self.conf_path, work_tag=work_tag, is_queue=True, **kwargs)

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
        if is_string(work_tag) is False:
            raise InvalidWorkerTag()
        if len(work_tag) <= 0:
            raise InvalidWorkerTag()
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

    def _push(self, key, params, work_tag, sub_key=None, report_tag=None, is_head=False):
        if work_tag is None:
            work_tag = self.work_tag
        v = self.package_task_info(work_tag, key, params, sub_key=sub_key, report_tag=report_tag)
        queue_key = self.queue_prefix_key + "_" + work_tag
        if is_head is True:
            self.redis_man.lpush(queue_key, v)
        else:
            self.redis_man.rpush(queue_key, v)

    def push(self, key, params, work_tag=None, sub_key=None, report_tag=None, is_head=False):
        key = "%s" % key
        if len(key) <= 0:
            raise InvalidTaskKey()
        self._push(key, params, work_tag, sub_key=sub_key, report_tag=report_tag, is_head=is_head)

    def push_head(self, key, params, work_tag=None, sub_key=None, report_tag=None):
        self.push(key, params, work_tag, sub_key=sub_key, report_tag=report_tag, is_head=True)

    def push_tail(self, key, params, work_tag=None, sub_key=None, report_tag=None):
        self.push(key, params, work_tag, sub_key=sub_key, report_tag=report_tag, is_head=False)

    def push_null_packages(self, work_tag=None, num=1):
        """
            add in version 0.6.8
        """
        while num > 0:
            self._push("", "", work_tag, is_head=True)
            num -= 1

    def wash_worker(self, work_tag=None, num=1):
        """
            add in version 0.6.5
        """
        self.push_null_packages(work_tag, num)


class RedisStat(RedisWorkerConfig, WorkerConfig):

    """
        class RedisStat
        add in version 0.9.1
    """
    conf_path_environ_key = "REDIS_WORKER_CONF_PATH"

    def __init__(self, conf_path=None, work_tag=None, redis_host=None, redis_password=None, redis_port=None,
                 redis_db=None, section_name="Redis", **kwargs):
        self.conf_path = conf_path
        if self.conf_path is None or os.path.exists(self.conf_path) is False:
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
        RedisWorkerConfig.__init__(self, self.conf_path, redis_host=redis_host, redis_password=redis_password,
                                   redis_port=redis_port, redis_db=redis_db, section_name=section_name)
        WorkerConfig.__init__(self, self.conf_path, work_tag=work_tag, is_queue=True, **kwargs)

    def list_queue(self):
        d_q = dict()
        qs = self.redis_man.keys(self.queue_prefix_key + "_*")
        len_k = len(self.queue_prefix_key) + 1
        for item in qs:
            if self.redis_man.type(item) == "list":
                l = self.redis_man.llen(item)
                d_q[item[len_k:]] = l
        return d_q

    def list_queue_detail(self, work_tag, limit=None):
        l_qd = []
        key = self.queue_prefix_key + "_" + work_tag
        t = self.redis_man.type(key)
        if t != "list":
            return None
        index = 0
        if isinstance(limit, int) is True and limit > 0:
            is_true = False
        else:
            limit = -1
            is_true = True
        while is_true or index < limit:
            v = self.redis_man.lindex(key, index)
            if v is None:
                break
            l_qd.append(v)
            index += 1
        return l_qd

    def list_worker(self):
        """
        add in version 0.9.7
        """
        d_w = dict()
        ws = self.redis_man.keys(self.clock_prefix_key + "_*")
        len_k = len(self.clock_prefix_key) + 1
        for item in ws:
            if self.redis_man.type(item) == "string":
                tag_id = item[len_k:]
                tag_id_s = tag_id.rsplit("_", 1)
                if len(tag_id_s) != 2:
                    continue
                tag = tag_id_s[0]
                w_id = tag_id_s[1]
                if tag not in d_w:
                    d_w[tag] = [w_id]
                else:
                    d_w[tag].append(w_id)
        return d_w

    def list_worker_detail(self, work_tag):
        """
        add in version 0.9.7
        """
        d_wd = dict()
        work_tag = StringTool.encode(work_tag)
        key = StringTool.join([self.clock_prefix_key, work_tag, "*"], "_").strip("_")
        len_k = len(self.clock_prefix_key) + 2 + len(work_tag)
        ws = self.redis_man.keys(key)
        for item in ws:
            if self.redis_man.type(item) != "string":
                continue
            pre_key = item[len_k:]
            if re.search(r"[^\da-z]", pre_key, re.I) is not None:
                continue
            p = dict()
            v = self.redis_man.get(item)
            p["value"] = v
            vs = v.split("_", 2)
            if len(vs) < 2:
                continue
            p["heartbeat_value"] = vs[0]
            p["clock_time"] = vs[1]
            if len(vs) > 2:
                p["current_task"] = vs[2]
                p["working"] = True
            else:
                p["working"] = False
            d_wd[pre_key] = p
        return d_wd


class RedisData(object):
    BOOL_VALUE = [False, True]

    @staticmethod
    def package_data(data):
        if data is None:
            return "n_"
        if isinstance(data, dict):
            return "d_" + json.dumps(data)
        if isinstance(data, list):
            return "l_" + json.dumps(data)
        if isinstance(data, bool):
            return "b_%s" % RedisData.BOOL_VALUE.index(data)
        if isinstance(data, six.integer_types):
            return "i_%s" % data
        if isinstance(data, float):
            return "f_%s" % data
        else:
            return "s_%s" % data

    @staticmethod
    def unpack_data(p_data):
        if is_string(p_data) is False:
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
        elif sign == "n":
            return None
        return p_data


class RedisWorker(RedisWorkerConfig, Worker):
    """
        expect_params_type
        add in version 0.1.8
    """
    conf_path_environ_key = "REDIS_WORKER_CONF_PATH"

    def __init__(self, conf_path=None, heartbeat_value=None, work_tag=None, log_dir=None, redis_host=None,
                 redis_password=None, redis_port=None, redis_db=None, section_name="Redis", **kwargs):
        self.conf_path = conf_path
        if self.conf_path is None or is_string(self.conf_path) is False or os.path.exists(self.conf_path) is False:
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
        RedisWorkerConfig.__init__(self, self.conf_path, redis_host=redis_host, redis_password=redis_password,
                                   redis_port=redis_port, redis_db=redis_db, section_name=section_name)
        Worker.__init__(self, conf_path=self.conf_path, work_tag=work_tag, log_dir=log_dir, **kwargs)
        if heartbeat_value is None:
            heartbeat_value = StringTool.random_str(str_len=12, upper_s=False)
        self.heartbeat_value = StringTool.decode(heartbeat_value)
        if re.match(r"^[\da-zA-Z]{3,50}$", self.heartbeat_value) is None:
            raise ValueError("heartbeat only allow 0-9 a-z and length between 3 and 50.")
        self.redis_man.set(self.heartbeat_key, heartbeat_value)
        t_clock = threading.Thread(target=self.hang_up_clock)
        t_clock.daemon = True
        t_clock.start()

    def has_heartbeat(self):
        current_value = StringTool.decode(self.redis_man.get(self.heartbeat_key))
        if current_value != self.heartbeat_value:
            self.worker_log("heartbeat is", self.heartbeat_value, "now is", current_value)
            return False
        return True

    def hang_up_clock(self, freq=None):
        loop_run = True
        if isinstance(freq, int) and freq >= 1:
            loop_run = False
        else:
            freq = 0
        key = self.clock_key
        hang_freq = 0
        while True:
            if self.is_running is False and loop_run is True:
                sleep(5)
                continue
            try:
                if self.current_task is not None and self.current_task.task_key is not None:
                    v = StringTool.join([self.heartbeat_value, int(time()), self.current_task.task_key], "_").strip("_")
                else:
                    v = StringTool.join([self.heartbeat_value, int(time())], "_").strip("_")
                self.redis_man.setex(key, v, 60)
            except RedisError:
                pass
            hang_freq += 1
            if hang_freq < freq or loop_run is True:
                sleep(55)
            else:
                break

    def hang_down_clock(self):
        key = "%s_%s_%s" % (self.clock_prefix_key, self.work_tag, self._id)
        self.redis_man.delete(key)

    def pop_task(self, freq=0):
        try:
            next_task = self.redis_man.blpop(self.queue_key, self.pop_time_out)
        except Exception as e:
            if freq > 5:
                self.worker_log(e, level="ERROR")
                raise e
            freq += 1
            sleep(10 * freq)
            return self.pop_task(freq)
        if next_task is not None:
            t = StringTool.decode(next_task[1])
            return t
        return next_task

    def push_task(self, key, params, work_tag=None, sub_key=None, report_tag=None, is_report=False):
        if work_tag is None:
            queue_key = self.queue_key
            work_tag = self.work_tag
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

    def has_task_item(self, item_index, hash_key=None, key=None, sub_key=None):
        item_key = self._task_item_key(item_index, key, sub_key)
        return self.redis_man.hexists(item_key, hash_key)

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
        if self.log_dir is None or is_string(self.log_dir) is False:
            return
        msg = StringTool.join(args, " ")
        level = kwargs.pop("level", "INFO")
        level = str(level).upper()
        if level not in ["INFO", "DEBUG"]:
            self.publish_message(msg)
        log_file = os.path.join(self.log_dir, "%s.log" % self.work_tag)
        now_time = datetime.now().strftime(TIME_FORMAT)
        write_a = ["[", self.heartbeat_value]
        if self.worker_index is not None:
            write_a.extend([":", self.worker_index])
        write_a.extend(["] ", now_time, ": ", level, " ", msg, "\n"])
        with open(log_file, "ab", 0) as wl:
            u = StringTool.join(write_a, join_str="")
            s = StringTool.encode(u)
            wl.write(s)
            if self.redirect_stdout is False and self.debug is True:
                try:
                    print(s)
                except Exception as e:
                    pass

    def task_log(self, *args, **kwargs):
        if self.log_dir is None or is_string(self.log_dir) is False:
            return
        if self.current_task is None or self.current_task.task_key is None:
            return
        msg = StringTool.join(args, " ")
        level = kwargs.pop("level", "INFO")
        level = str(level).upper()
        if level not in ["INFO", "DEBUG"]:
            p_msg = StringTool.join([self.current_task.task_key, "\n", msg], "")
            self.publish_message(p_msg)
        log_file = os.path.join(self.log_dir, "%s_%s.log" % (self.work_tag, self.current_task.task_key))
        now_time = datetime.now().strftime(TIME_FORMAT)
        write_a = ["[", self.heartbeat_value]
        if self.worker_index is not None:
            write_a.extend([":", self.worker_index])
        if self.current_task.task_sub_key is not None:
            write_a.extend(["][", self.current_task.task_sub_key])
        write_a.extend(["] ", now_time, ": ", level, " ", msg, "\n"])
        with open(log_file, "ab", 0) as wl:
            u = StringTool.join(write_a, join_str="")
            s = StringTool.encode(u)
            wl.write(s)
            if self.redirect_stdout is False and self.debug is True:
                try:
                    print(s)
                except Exception as e:
                    pass

    def handle_invalid_task(self, task_info, error_info):
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
        if len(keys[0]) <= 0:
            return True, None
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
        if partition_task[2] == "report":
            task_item.set(is_report_task=True)
            task_item.set(task_params=WorkerTask(**params))
        else:
            if self.expect_params_type is not None:
                if not isinstance(params, self.expect_params_type):
                    return False, "Invalid task, not expect param type"
            if isinstance(self.expect_params_type, dict) is True:
                task_item.set(task_params=WorkerTaskParams(**params))
                task_item.task_params.debug_func = self.task_debug_log
            else:
                task_item.set(task_params=params)
        return True, task_item

    def run(self):
        if self.is_running is True:
            self.worker_log("Is Running")
            return False
        self.is_running = True
        self.worker_log("Start Run Worker")
        self.worker_log("Worker Conf Path Is ", self.conf_path)
        self.worker_log("Worker Heartbeat Value Is", self.heartbeat_value)
        self.worker_log("Worker Work Tag Is ", self.work_tag)
        self.worker_log("Worker QueHeartbeat Key Is", self.heartbeat_key)
        self.worker_log("Worker Queue Key Is", self.queue_key)
        self.worker_log("Worker Clock Key Is", self.clock_key)

        while True:
            if self.has_heartbeat() is False:
                self.close()
            next_task = self.pop_task()
            if next_task is None:
                continue
            parse_r, task_item = self.parse_task_info(next_task)
            if parse_r is False:
                self.handle_invalid_task(next_task, task_item)
                self.num_wrongful_job += 1
                continue
            elif task_item is None:
                self.worker_log("Receive Null Package")
                self.num_null_job += 1
                continue
            if isinstance(task_item, WorkerTask):
                self.current_task = task_item
            else:
                continue
            self._execute()
