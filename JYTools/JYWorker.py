#! /usr/bin/env python
# coding: utf-8

import os
import sys
import types
import ConfigParser
from datetime import datetime
import json
import traceback
from redis import Redis
from JYTools import TIME_FORMAT
from JYTools import StringTool

__author__ = 'meisanggou'


class _WorkerConfig(object):
    """
        [Worker]
        heartbeat_prefix_key: worker_heartbeat
        queue_prefix_key: task_queue
        work_tag: jy_task
        pop_time_out: 60
    """
    def __init__(self, conf_path=None, section_name="Worker", **kwargs):
        self.heartbeat_prefix_key = "worker_heartbeat"
        self.work_tag = "jy_task"
        self.worker_index = None
        self.queue_prefix_key = "task_queue"
        self.pop_time_out = 60
        if conf_path is not None:
            self.load_work_config(conf_path, section_name)
        if "work_tag" in kwargs:
            self.work_tag = kwargs["work_tag"]
        if "worker_index" in kwargs:
            self.worker_index = kwargs["worker_index"]
        self.heartbeat_key = self.heartbeat_prefix_key + "_" + self.work_tag
        self.queue_key = self.queue_prefix_key + "_" + self.work_tag
        if self.heartbeat_key == self.queue_key:
            self.heartbeat_key = "heartbeat_" + self.heartbeat_key
        self.current_task = None
        self.current_key = None

    def load_work_config(self, conf_path, section_name):
        config = ConfigParser.ConfigParser()
        config.read(conf_path)
        if config.has_section(section_name):
            if config.has_option(section_name, "heartbeat_prefix_key"):
                self.heartbeat_prefix_key = config.get(section_name, "heartbeat_prefix_key")
            if config.has_option(section_name, "work_tag"):
                self.work_tag = config.get(section_name, "work_tag")
            if config.has_option(section_name, "queue_prefix_key"):
                self.queue_prefix_key = config.get(section_name, "queue_prefix_key")
            if config.has_option(section_name, "pop_time_out"):
                self.pop_time_out = config.getint(section_name, "pop_time_out")

    def set_work_tag(self, work_tag):
        """
            add in version 0.1.14
        """
        self.work_tag = work_tag
        self.heartbeat_key = self.heartbeat_prefix_key + "_" + self.work_tag
        self.queue_key = self.queue_prefix_key + "_" + self.work_tag


class _WorkerLogConfig(object):
    def __init__(self, **kwargs):
        self.log_dir = None
        if "log_dir" in kwargs:
            self.log_dir = kwargs["log_dir"]


class _WorkerLog(_WorkerLogConfig):
    def worker_log(self, *args, **kwargs):
        print(args)

    def task_log(self, *args, **kwargs):
        print(args)


class _Worker(_WorkerConfig, _WorkerLog):
    def __init__(self, **kwargs):
        _WorkerConfig.__init__(self, **kwargs)
        _WorkerLog.__init__(self, **kwargs)

    def has_heartbeat(self):
        return True

    def execute(self, key, args):
        try:
            self.handler_task(key, args)
        except Exception as e:
            self.task_log(traceback.format_exc(), level="ERROR")
            self.execute_error(e)

    def execute_error(self, e):
        if self.handler_task_exception is not None:
            self.handler_task_exception(e)

    # 子类需重载的方法
    def handler_task(self, key, params):
        pass

    # 子类需重载的方法
    def handler_task_exception(self, e):
        pass

    def handler_invalid_task(self, task_info, error_info):
        pass

    def close(self, exit_code=0):
        self.worker_log("start close. exit code: %s" % exit_code)
        exit(exit_code)


class _RedisWorkerConfig(object):
    """
        [Redis]
        redis_host: localhost
        redis_port: 6379
        redis_password:
        redis_db: 13
    """

    def __init__(self, conf_path=None, section_name="Redis"):
        self.redis_host = "localhost"
        self.redis_port = 6379
        self.redis_password = None
        self.redis_db = 13
        if conf_path is not None:
            self.load_redis_config(conf_path, section_name)
        if self.redis_password == "":
            self.redis_password = None
        self.redis_man = Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db,
                               password=self.redis_password)

    def load_redis_config(self, conf_path, section_name):
        config = ConfigParser.ConfigParser()
        config.read(conf_path)
        if config.has_section(section_name):
            if config.has_option(section_name, "redis_host"):
                self.redis_host = config.get(section_name, "redis_host")
            if config.has_option(section_name, "redis_port"):
                self.redis_port = config.getint(section_name, "redis_port")
            if config.has_option(section_name, "redis_password"):
                self.redis_password = config.get(section_name, "redis_password")
            if config.has_option(section_name, "redis_db"):
                self.redis_db = config.getint(section_name, "redis_db")


class RedisQueue(_RedisWorkerConfig, _WorkerConfig):
    def __init__(self, conf_path, **kwargs):
        _RedisWorkerConfig.__init__(self, conf_path)
        _WorkerConfig.__init__(self, conf_path, **kwargs)

    def package_task_info(self, key, args):
        """
        info format: wor_tag,key,args_type,args
        args_type: json
        example: jy_task,key_1,json,{"v":1}
        """
        v = "%s,%s," % (self.work_tag, key)
        if isinstance(args, dict):
            v += "json," + json.dumps(args)
        else:
            v += "string," + args
        return v

    def push_head(self, key, params, work_tag=None):
        v = self.package_task_info(key, params)
        self.redis_man.lpush(self.queue_key, v)

    def push_tail(self, key, params, work_tag=None):
        v = self.package_task_info(key, params)
        self.redis_man.rpush(self.queue_key, v)

    def push(self, key, params, work_tag=None):
        self.push_tail(key, params, work_tag)


class RedisWorker(_RedisWorkerConfig, _Worker):
    """
        expect_params_type
        add in version 0.1.8
    """
    expect_params_type = None

    def __init__(self, conf_path=None, heartbeat_value="0", **kwargs):
        _RedisWorkerConfig.__init__(self, conf_path)
        _Worker.__init__(self, conf_path=conf_path, **kwargs)
        self.heartbeat_value = StringTool.decode(heartbeat_value)
        self.redis_man.set(self.heartbeat_key, heartbeat_value)
        self._msg_manager = None

    def has_heartbeat(self):
        current_value = StringTool.decode(self.redis_man.get(self.heartbeat_key))
        if current_value != self.heartbeat_value:
            self.worker_log("heartbeat is", self.heartbeat_value, "now is", current_value)
            return False
        return True

    def pop_task(self):
        next_task = self.redis_man.blpop(self.queue_key, self.pop_time_out)
        if next_task is not None:
            return next_task[1]
        return next_task

    def push_task(self, task_info, work_tag=None):
        if work_tag is None:
            queue_key = self.queue_key
        else:
            queue_key = self.queue_prefix_key + "_" + work_tag
        self.redis_man.rpush(queue_key, task_info)

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
            self.publish_message("%s\n%s" % (self.current_key, msg))
        log_file = os.path.join(self.log_dir, "%s_%s.log" % (self.work_tag, self.current_key))
        now_time = datetime.now().strftime(TIME_FORMAT)
        write_a = ["[", self.heartbeat_value]
        if self.worker_index is not None:
            write_a.extend([":", self.worker_index])
        write_a.extend(["] ", now_time, ": ", level, " ", msg, "\n"])
        with open(log_file, "a", 0) as wl:
            s = StringTool.join(write_a, join_str="")
            s = StringTool.encode(s)
            wl.write(s)

    @property
    def msg_manager(self):
        return self._msg_manager

    @msg_manager.setter
    def msg_manager(self, msg_manager):
        if msg_manager is None:
            return
        if hasattr(msg_manager, "publish_message") is False:
            return
        if isinstance(msg_manager.publish_message, types.MethodType) is False:
            return
        self._msg_manager = msg_manager

    def publish_message(self, message):
        """

        add in version 0.1.4
        """
        if self.msg_manager is None:
            return
        try:
            self.msg_manager.publish_message(message, self.work_tag)
        except Exception as e:
            print(e)

    def handler_invalid_task(self, task_info, error_info):
        self.worker_log(error_info, level="WARING")

    def set_current_task_invalid(self, *args):
        """
            add in version 0.1.14
        """
        if self.current_task is not None:
            self.worker_log("Invalid Task ", self.current_task, " Error Info: ", *args, level="WARING")

    def parse_task_info(self, task_info):
        partition_task = task_info.split(",", 3)
        if len(partition_task) != 4:
            error_msg = "Invalid task %s, task partition length is not 3" % task_info
            return False, error_msg
        if partition_task[0] != self.work_tag:
            error_msg = "Invalid task %s, task not match work tag %s" % (task_info, self.work_tag)
            return False, error_msg
        if partition_task[2] not in ("string", "json"):
            error_msg = "Invalid task %s, task args type invalid" % task_info
            return False, error_msg
        key = partition_task[1]
        params = partition_task[3]
        if partition_task[2] == "json":
            try:
                params = json.loads(params)
            except ValueError:
                error_msg = "Invalid task %s, task args type and args not uniform" % task_info
                return False, error_msg
        if self.expect_params_type is not None:
            if not isinstance(params, self.expect_params_type):
                return False, "Invalid task, not expect param type"
        return True, [key, params]

    def run(self):
        self.worker_log("Start Run Worker")
        while True:
            if self.has_heartbeat() is False:
                self.close()
            next_task = self.pop_task()
            if next_task is None:
                continue
            parse_r, task_args = self.parse_task_info(next_task)
            if parse_r is False:
                self.handler_invalid_task(next_task, task_args)
                continue
            self.current_task = next_task
            self.current_key = task_args[0]
            self.worker_log("Start Execute", self.current_key)
            self.execute(task_args[0], task_args[1])
            self.worker_log("Completed Task", self.current_key)

    def work(self, daemon=False):
        """
        add in version 0.1.8
        """
        if daemon is True:
            try:
                pid = os.fork()
                if pid == 0:  # pid大于0代表是父进程 返回的是子进程的pid pid==0为子进程
                    self.run()
            except OSError as e:
                sys.exit(1)
        else:
            self.run()

if __name__ == "__main__":
    r_worker = RedisWorker(log_dir="/tmp", heartbeat_value="中文", worker_index=1)
    print(r_worker.log_dir)
    print(r_worker.work_tag)
    r_worker.work()
