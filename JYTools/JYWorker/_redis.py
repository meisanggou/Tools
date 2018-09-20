#! /usr/bin/env python
# coding: utf-8
import os
import signal
import argparse
import json
import threading
import logging
import re
import six
import time
from datetime import datetime
from redis import RedisError
from JYTools import TIME_FORMAT
from JYTools import StringTool
from JYTools.StringTool import is_string
from ._config import RedisWorkerConfig, WorkerConfig
from .util import ValueVerify, ReportScene
from ._Worker import Worker
from ._Task import WorkerTask, WorkerTaskParams, TaskType, TaskStatus
from ._exception import InvalidTaskKey, InvalidWorkTag

__author__ = '鹛桑够'


class _RedisHelper(RedisWorkerConfig, WorkerConfig):
    conf_path_environ_key = "REDIS_WORKER_CONF_PATH"

    def __init__(self, conf_path=None, work_tag=None, redis_host=None, redis_password=None, redis_port=None,
                 redis_db=None, section_name="Redis", redis_man=None, **kwargs):
        self.conf_path = conf_path
        if self.conf_path is None or os.path.exists(self.conf_path) is False:
            logging.debug("Conf Path %s Not Exist ", self.conf_path)
            logging.debug("Read os environ : %s", self.conf_path_environ_key)
            env_conf_path = os.environ.get(self.conf_path_environ_key)
            logging.debug("os environ %s %s", self.conf_path_environ_key, env_conf_path)
            if env_conf_path is not None:
                if os.path.exists(env_conf_path) is True:
                    self.conf_path = env_conf_path
                    logging.debug("Use %s As conf path", env_conf_path)
                else:
                    logging.debug("Path %s Not Exist", env_conf_path)
        RedisWorkerConfig.__init__(self, self.conf_path, redis_host=redis_host, redis_password=redis_password,
                                   redis_port=redis_port, redis_db=redis_db, section_name=section_name,
                                   redis_man=redis_man)
        WorkerConfig.__init__(self, self.conf_path, work_tag=work_tag, **kwargs)


class RedisQueue(_RedisHelper):
    """
        conf_path_environ_key
        add in version 0.1.25
    """

    @staticmethod
    def package_task_info(work_tag, key, params, sub_key=None, report_tag=None, report_scene=ReportScene.END,
                          is_report=False):
        """
        info format: work_tag[|report_tag[:report_scene]],key[|sub_key],args_type(task_type),args
        args_type: json
        example: jy_task,key_1,json,{"v":1}
        example: jy_task|ping,key_1|first,json,{"v":1}
        """
        if sub_key is not None:
            key = "%s|%s" % (key, sub_key)
        if is_string(work_tag) is False:
            raise InvalidWorkTag()
        if ValueVerify.v_work_tag(work_tag) is False:
            raise InvalidWorkTag()
        if report_tag is not None:
            if ValueVerify.v_report_tag(report_tag) is False:
                raise InvalidWorkTag()
            work_tag = "%s|%s:%s" % (work_tag, report_tag, report_scene)
        v = "%s,%s," % (work_tag, key)
        if isinstance(params, dict):
            if is_report is False:
                v += "json," + json.dumps(params)
            else:
                v += "report," + json.dumps(params)
        else:
            v += "string," + params
        return v

    @staticmethod
    def package_task(work_tag, key, params, sub_key=None, task_type=TaskType.Normal, **kwargs):
        """
        info format: work_tag[|report_tag[:report_scene]],key[|sub_key],args_type(task_type),args
        args_type: json
        example: jy_task,key_1,json,{"v":1}
        example: jy_task|ping,key_1|first,json,{"v":1}
        """
        if sub_key is not None:
            key = "%s|%s" % (key, sub_key)
        if is_string(work_tag) is False:
            raise InvalidWorkTag()
        if ValueVerify.v_work_tag(work_tag) is False:
            raise InvalidWorkTag()
        if task_type == TaskType.Normal:
            report_tag = kwargs.pop("report_tag", None)
            if report_tag is not None:
                if ValueVerify.v_report_tag(report_tag) is False:
                    raise InvalidWorkTag()
                report_scene = kwargs.pop("report_scene", ReportScene.END)
                work_tag = "%s|%s:%s" % (work_tag, report_tag, report_scene)
            if isinstance(params, dict):
                args_s = "json," + json.dumps(params)
            else:
                args_s = "string," + json.dumps(params)
        elif task_type == TaskType.Control:
            if "expected_status" not in params:
                raise RuntimeError("Not found expected_status in params")
            expected_status = TaskStatus.parse(params["expected_status"])
            if expected_status is None:
                raise RuntimeError("Invalid expected_status")
            params["expected_status"] = expected_status
            args_s = "control," + json.dumps(params)
        elif task_type == TaskType.Report:
            args_s = "report," + json.dumps(params)
        else:
            raise RuntimeError("Invalid task type")
        v = "%s,%s,%s" % (work_tag, key, args_s)
        return v

    def _push(self, key, params, work_tag, sub_key=None, report_tag=None, is_head=False, is_report=False):
        if work_tag is None:
            work_tag = self.work_tag
        v = self.package_task_info(work_tag, key, params, sub_key=sub_key, report_tag=report_tag, is_report=is_report)
        queue_key = self.queue_prefix_key + "_" + work_tag
        if is_head is True:
            self.redis_man.lpush(queue_key, v)
        else:
            self.redis_man.rpush(queue_key, v)

    def push_control(self, key, work_tag, expected_status, sub_key=None, **params):
        if work_tag is None:
            work_tag = self.work_tag
        params.update(expected_status=expected_status)
        v = self.package_task(work_tag, key, params, sub_key=sub_key, task_type=TaskType.Control)
        queue_key = self.queue_prefix_key + "_" + work_tag
        self.redis_man.rpush(queue_key, v)

    def push(self, key, params, work_tag=None, sub_key=None, report_tag=None, is_head=False, is_report=False):
        key = "%s" % key
        if len(key) <= 0:
            raise InvalidTaskKey()
        self._push(key, params, work_tag, sub_key=sub_key, report_tag=report_tag, is_head=is_head, is_report=is_report)

    def push_file(self, key, file_path, work_tag=None, sub_key=None, report_tag=None, is_head=False):
        with open(file_path) as r:
            c = r.read()
            params = json.loads(c)
        return self.push(key, params, work_tag, sub_key=sub_key, report_tag=report_tag, is_head=is_head)

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


class RedisStat(_RedisHelper):

    """
        class RedisStat
        add in version 0.9.1
    """

    def list_queue(self):
        d_q = dict()
        qs = self.redis_man.keys(self.queue_prefix_key + "_*")
        len_k = len(self.queue_prefix_key) + 1
        for item in qs:
            if self.redis_man.type(item) == "list":
                l = self.redis_man.llen(item)
                d_q[item[len_k:]] = l
            elif self.redis_man.type(item) == "zset" and item.endswith("@delay"):
                l = self.redis_man.zcard(item)
                d_q[item[len_k:]] = l
        return d_q

    def list_queue_detail(self, work_tag, limit=None):
        l_qd = []
        key = self.queue_prefix_key + "_" + work_tag
        t = self.redis_man.type(key)
        if t != "list":
            return []
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

    def remove_queue_task(self, work_tag, key, report_tag=None, sub_key=None):
        if report_tag is not None:
            re_work_tag = StringTool.join_decode([work_tag, report_tag], join_str="|")
        else:
            re_work_tag = work_tag
        if sub_key is not None:
            key = StringTool.join_decode([key, sub_key], join_str="|")
        value_prefix = StringTool.join_decode([re_work_tag, key], ",")
        queue_tasks = self.list_queue_detail(work_tag)
        if queue_tasks is None:
            return 0
        count = 0
        key = StringTool.join_decode([self.queue_prefix_key, work_tag], join_str="_")
        for task in queue_tasks:
            if task.startswith(value_prefix) is True:
                try:
                    count += self.redis_man.lrem(key, task, num=0)
                except Exception:
                    continue
        return count

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
            try:
                p["clock_time"] = int(p["clock_time"])
                x = time.localtime(p["clock_time"])
                p["clock_time2"] = time.strftime("%Y-%m-%d %H:%M:%S", x)
            except ValueError:
                pass
            if len(vs) > 2:
                p["current_task"] = vs[2]
                p["working"] = True
            else:
                p["working"] = False
            d_wd[pre_key] = p
        return d_wd

    def list_heartbeat(self):
        """
        add in version 1.0.6
        """
        l_h = []
        key_prefix = StringTool.join_decode([self.heartbeat_prefix_key, "_*"])
        len_k = len(key_prefix) - 1
        hs = self.redis_man.keys(key_prefix)
        for item in hs:
            if self.redis_man.type(item) == "string":
                tag = item[len_k:]
                if len(tag) > 0:
                    l_h.append(tag)
        return l_h

    def list_heartbeat_detail(self, work_tag):
        """
        add in version 1.0.6
        """
        key = StringTool.join_encode([self.heartbeat_prefix_key, "_", work_tag])
        print(key)
        return self.redis_man.get(key)

    def delete_heartbeat(self, work_tag):
        key = StringTool.join_encode([self.heartbeat_prefix_key, "_", work_tag])
        return self.redis_man.delete(key)

    def list_worry_queue(self):
        w_q = dict()
        d_q = self.list_queue()
        for k, v in d_q.items():
            d_wd = self.list_worker_detail(k)
            if len(d_wd.keys()) <= 0:
                w_q[k] = v
        return w_q

    def list_task_item(self, work_tag, key, sub_key=None):
        k_l = [self.queue_prefix_key, work_tag, key]
        if sub_key is not None:
            k_l.append(sub_key)
        task_item_compile = re.compile(re.escape(StringTool.join_decode(k_l, "_")) + "_(\\d+)$")
        get_key = StringTool.join_decode([k_l], "_")
        k_l.append("*")
        key_prefix = StringTool.join_decode(k_l, "_")
        hs = self.redis_man.keys(key_prefix)
        task_items = dict(sub=dict(), values=dict())
        for item in hs:
            if self.redis_man.type(item) != "hash":
                continue
            m_r = task_item_compile.match(item)
            if m_r is None:
                continue
            task_items["sub"][m_r.groups()[0]] = item
        if self.redis_man.type(get_key) == "hash":
            item = self.redis_man.hgetall(get_key)
            for key in item.keys():
                task_items["values"][key] = RedisData.unpack_data(item[key])
        return task_items

    def clear_task_item(self, work_tag, key):
        k_l = [self.queue_prefix_key, work_tag, key, "*"]
        key_prefix = StringTool.join_decode(k_l, "_")
        hs = self.redis_man.keys(key_prefix)
        task_items = dict(sub=dict(), values=dict())
        for item in hs:
            if self.redis_man.type(item) != "hash":
                continue
            self.redis_man.delete(item)
        return task_items


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
    usage = "RedisWorker use redis to async execute"
    description = "Please use follow arguments init worker"
    init_parser = argparse.ArgumentParser(usage=usage, description=description)
    test_parser = init_parser.add_argument_group("test arguments")
    work_parser = init_parser.add_argument_group("work arguments")
    other_parser = init_parser.add_argument_group("other arguments")

    def __init__(self, conf_path=None, heartbeat_value=None, is_brother=False, work_tag=None, log_dir=None,
                 redis_host=None, redis_password=None, redis_port=None, redis_db=None, section_name="Redis", **kwargs):
        self.conf_path = conf_path
        if self.conf_path is None or is_string(self.conf_path) is False or os.path.exists(self.conf_path) is False:
            logging.debug("Conf Path %s Not Exist ", self.conf_path)
            logging.debug("Read os environ : %s", self.conf_path_environ_key)
            env_conf_path = os.environ.get(self.conf_path_environ_key)
            logging.debug("os environ %s %s", self.conf_path_environ_key, env_conf_path)
            if env_conf_path is not None:
                if os.path.exists(env_conf_path) is True:
                    self.conf_path = env_conf_path
                    logging.debug("Use %s As conf path", env_conf_path)
                else:
                    logging.debug("Path %s Not Exist", env_conf_path)
        RedisWorkerConfig.__init__(self, self.conf_path, redis_host=redis_host, redis_password=redis_password,
                                   redis_port=redis_port, redis_db=redis_db, section_name=section_name)
        Worker.__init__(self, conf_path=self.conf_path, work_tag=work_tag, log_dir=log_dir, **kwargs)
        self.stat_man = RedisStat(conf_path=self.conf_path, redis_host=redis_host, redis_password=redis_password,
                                  redis_port=redis_port, redis_db=redis_db, section_name=section_name)
        if is_brother is True:
            current_heartbeat = self.redis_man.get(self.heartbeat_key)
            if current_heartbeat is not None:
                heartbeat_value = current_heartbeat
        if heartbeat_value is None:
            heartbeat_value = StringTool.random_str(str_len=12, upper_s=False)
        self.heartbeat_value = StringTool.decode(heartbeat_value)
        if ValueVerify.v_heartbeat(self.heartbeat_value) is False:
            raise ValueError("heartbeat only allow 0-9 a-z and length between 3 and 50.")

        self.t_clock = threading.Thread(target=self.hang_up_clock)
        self.t_clock.daemon = True  # 主进程退出时，随主进程一起退出
        if "upload_log_tag" in kwargs:
            self.upload_log_tag = kwargs["upload_log_tag"]
        else:
            self.upload_log_tag = None

    def set_heartbeat(self):
        self.redis_man.set(self.heartbeat_key, self.heartbeat_value)

    def has_heartbeat(self):
        current_value = StringTool.decode(self.redis_man.get(self.heartbeat_key))
        if current_value != self.heartbeat_value:
            self.worker_log("heartbeat is", self.heartbeat_value, "now is", current_value)
            return False
        return True

    def hang_up_clock(self, freq=None):
        # test模式下或者还没有运行，is_running为False不进行打卡
        if self.is_running is False:
            return
        loop_run = True
        if isinstance(freq, int) and freq >= 1:
            loop_run = False
        else:
            freq = 0
        key = self.clock_key
        hang_freq = 0
        while True:
            # run以后才启动线程，所以此判断可以不用了
            # if self.is_running is False and loop_run is True:
            #     time.sleep(5)
            #     continue
            try:
                if self.current_task is not None and self.current_task.task_key is not None:
                    v = StringTool.join([self.heartbeat_value, int(time.time()), self.current_task.task_key], "_").strip("_")
                else:
                    v = StringTool.join([self.heartbeat_value, int(time.time())], "_").strip("_")
                self.redis_man.setex(key, v, 60)
            except RedisError:
                pass
            hang_freq += 1
            if hang_freq < freq or loop_run is True:
                time.sleep(55)
            else:
                break

    def hang_down_clock(self):
        key = "%s_%s_%s" % (self.clock_prefix_key, self.work_tag, self._id)
        self.redis_man.delete(key)

    def pop_task(self, freq=0):
        self.num_pop_task += 1
        next_task = None
        try:
            if self.num_pop_task % 5 != 0:
                tasks = self.redis_man.blpop(self.queue_key, self.pop_time_out)
                if tasks is not None:
                    next_task = tasks[1]
            else:
                tasks = self.redis_man.zrangebyscore(self.delay_queue_key, 0, time.time())
                for item in tasks:
                    l = self.redis_man.zrem(self.delay_queue_key, item)
                    if l > 0:
                        next_task = item
                        break
        except Exception as e:
            if freq > 5:
                self.worker_log("[REDIS POP ERROR MSG]", e, level="ERROR")
                raise e
            time.sleep(5 * freq + 10)
            return self.pop_task(freq=freq+1)
        if next_task is not None:
            t = StringTool.decode(next_task)
            return t
        return next_task

    def _push_to_delay_queue(self, task_info):
        key = self.delay_queue_key
        delay_second = 30
        self.redis_man.zadd(key, task_info, time.time() + delay_second)

    def _push_to_queue(self, task_info, queue_key=None, is_head=False, freq=0):
        if queue_key is None:
            queue_key = self.queue_key
        try:
            if is_head is True:
                self.redis_man.lpush(queue_key, task_info)
            else:
                self.redis_man.rpush(queue_key, task_info)
        except RedisError as error:
            if freq > 5:
                self.worker_log("[REDIS PUSH ERROR DATA]", task_info, level="ERROR")
                self.worker_log("[REDIS PUSH ERROR MSG]", error, level="ERROR")
                raise error
            time.sleep(5 * freq + 10)
            self._push_to_queue(task_info, queue_key, is_head, freq=freq+1)

    def _push_task(self, key, params, work_tag=None, sub_key=None, report_tag=None, is_report=False,
                   report_scene=ReportScene.END, is_head=False):
        if work_tag is None:
            queue_key = self.queue_key
            work_tag = self.work_tag
        else:
            queue_key = self.queue_prefix_key + "_" + work_tag
        task_info = RedisQueue.package_task_info(work_tag, key, params, sub_key=sub_key, report_tag=report_tag,
                                                 is_report=is_report, report_scene=report_scene)
        self._push_to_queue(task_info, queue_key=queue_key, is_head=is_head)

    def push_task(self, key, params, work_tag=None, sub_key=None, report_tag=None, is_report=False,
                  report_scene=ReportScene.END):
        self._push_task(key, params, work_tag, sub_key, report_tag, is_report, report_scene)

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

    def del_task_item(self, item_index, hash_key=None, key=None, sub_key=None):
        item_key = self._task_item_key(item_index, key, sub_key)
        if hash_key is not None:
            l = self.redis_man.hdel(item_key, hash_key)
        else:
            l = self.redis_man.delete(item_key)
        return l

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
                    logging.info(s)
                except Exception as e:
                    pass

    def task_log(self, *args, **kwargs):
        if self.current_task is None or self.current_task.log_path is None:
            return
        msg = StringTool.join(args, " ")
        level = kwargs.pop("level", "INFO")
        level = str(level).upper()
        if level not in ["INFO", "DEBUG"]:
            p_msg_a = [self.current_task.task_key]
            if self.current_task.task_sub_key is not None:
                p_msg_a.extend([" ", self.current_task.task_sub_key])
            p_msg = StringTool.join([p_msg_a, "\n", msg], "")
            self.publish_message(p_msg)
            if self.upload_log_tag is not None:
                upload_info = dict(log_path=self.current_task.log_path, timestamp=int(time.time()))
                self.push_task(StringTool.join_decode([self.current_task.task_key, self.work_tag], join_str="_"),
                               upload_info, work_tag=self.upload_log_tag)
        log_file = self.current_task.log_path
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
                    logging.info(s)
                except Exception as e:
                    pass

    def handle_invalid_task(self, task_info, error_info):
        self.worker_log(error_info, level="WARNING")

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

        if partition_task[2] not in ("string", "json", "report", "control"):
            error_msg = "Invalid task %s, task args type invalid" % task_info
            return False, error_msg
        params = partition_task[3]
        if partition_task[2] in ("json", "report", "control"):
            try:
                params = json.loads(params)
            except ValueError:
                error_msg = "Invalid task %s, task args type and args not uniform" % task_info
                return False, error_msg
        if partition_task[2] == "report":
            task_item.set(is_report_task=True)
            task_item.set(task_params=WorkerTask(**params))
        elif partition_task[2] == "control":
            task_item.set(task_type=TaskType.Control)
            if "expected_status" not in params:
                return False, "Invalid Task, not found expected_status in params"
            expected_status = TaskStatus.parse(params["expected_status"])
            if expected_status is None:
                return False, "Invalid Task, unknown expected status, %s" % params["expected_status"]
            task_item.set(task_params=WorkerTaskParams(**params))
            task_item.task_params.debug_func = self.task_debug_log
        else:
            if self.expect_params_type is not None:
                if not isinstance(params, self.expect_params_type):
                    return False, "Invalid task, not expect param type"
            if isinstance(self.expect_params_type, dict) is True:
                task_item.set(task_params=WorkerTaskParams(**params))
                task_item.task_params.debug_func = self.task_debug_log
            else:
                task_item.set(task_params=params)
        if StringTool.is_string(self.log_dir) is True:
            log_name = StringTool.join_encode([self.work_tag, "_", task_item.task_key, ".log"], join_str="")
            task_item.log_path = StringTool.path_join(self.log_dir, log_name)
        return True, task_item

    def run(self, wash_old=False):
        if self.is_running is True:
            self.worker_log("Is Running")
            return False
        self._worker_status = 10
        # 发送空包清洗旧的worker
        self._push_task("", "", is_head=True)
        # 启动前其他辅助 运行起来：设置心跳值 打卡
        self.is_running = True
        self.t_clock.start()
        self.set_heartbeat()

        self.worker_log("Start Run Worker")
        self.worker_log("Worker Conf Path Is ", self.conf_path)
        self.worker_log("Worker Heartbeat Value Is", self.heartbeat_value)
        self.worker_log("Worker Work Tag Is ", self.work_tag)
        self.worker_log("Worker QueHeartbeat Key Is", self.heartbeat_key)
        self.worker_log("Worker Queue Key Is", self.queue_key)
        self.worker_log("Worker Clock Key Is", self.clock_key)

        while True:
            self._worker_status = 100
            if self.has_heartbeat() is False:
                self.close()
            self._worker_status = 110
            next_task = self.pop_task()
            self._worker_status = 120
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
            self._worker_status = 150
            self._execute()
            self._worker_status = 200

    def handle_sign(self, sign, frame):
        self.task_log("Redis Worker Receive SIGN", sign)
        if sign not in (signal.SIGUSR1, signal.SIGUSR2):
            if self.current_task is not None:
                self._push_to_delay_queue(self.current_task.task_info)
        self.worker_log("call close, because receive sign", sign)
        self.close(sign)

    @classmethod
    def parse_args(cls):
        cls.init_parser.add_argument("-b", "--heartbeat-value", dest="heartbeat_value", help="heartbeat value")
        cls.init_parser.add_argument("-c", "--worker-conf-path", "--redis_worker-conf-path", dest="conf_path",
                                     help="redis worker conf path")
        cls.init_parser.add_argument("-l", "--log-dir", dest="log_dir", help="worker log save dir")
        cls.init_parser.add_argument("-w", "--work-tag", dest="work_tag", help="work tag")
        cls.init_parser.add_argument("--debug", dest="debug", help="debug mode, print debug msg", action="store_true",
                                     default=False)
        cls.test_parser.add_argument("-e", "--example-path", dest="example_path", help="run an example use this file")
        cls.test_parser.add_argument("-k", "--key", dest="key", help="task key")
        cls.test_parser.add_argument("-r", "--report-tag", dest="report_tag", help="report tag")
        cls.test_parser.add_argument("-s", "--sub-key", dest="sub_key", help="task sub key")

        cls.work_parser.add_argument("-D", "--daemon", dest="daemon", help="work in daemon", action="store_true",
                                     default=False)

        args = cls.init_parser.parse_args()
        return args
