#! /usr/bin/env python
# coding: utf-8

import os
import types
import ConfigParser
from datetime import datetime
import json
import traceback
from redis import Redis
from JYTools import TIME_FORMAT

__author__ = 'meisanggou'


class _WorkerConfig(object):
    """
        [Worker]
        heartbeat_prefix_key: jy_work_heartbeat
        queue_prefix_key: jy_task_queue
        work_tag: jy_task
        pop_time_out: 60
    """
    def __init__(self, conf_path=None, section_name="Worker", **kwargs):
        self.heartbeat_prefix_key = "jy_work_heartbeat"
        self.work_tag = "jy_task"
        self.queue_prefix_key = "jy_task_queue"
        self.pop_time_out = 60
        if conf_path is not None:
            self.load_work_config(conf_path, section_name)
        if "work_tag" in kwargs:
            self.work_tag = kwargs["work_tag"]
        self.heartbeat_key = self.heartbeat_prefix_key + "_" + self.work_tag
        self.queue_key = self.queue_prefix_key + "_" + self.work_tag
        self.current_task = None
        self.log_dir = None
        if "log_dir" in kwargs:
            self.log_dir = kwargs["log_dir"]

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


class _Worker(_WorkerConfig):

    def has_heartbeat(self):
        return True

    def worker_log(self, msg, level="INFO"):
        print(msg)

    def task_log(self, msg, level="INFO"):
        print(msg)

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
    def handler_task(self, key, args):
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

    def push_head(self, key, args):
        v = self.package_task_info(key, args)
        self.redis_man.lpush(self.queue_key, v)

    def push_tail(self, key, args):
        v = self.package_task_info(key, args)
        self.redis_man.rpush(self.queue_key, v)

    def push(self, key, args):
        self.push_tail(key, args)


class RedisWorker(_RedisWorkerConfig, _Worker):
    def __init__(self, conf_path=None, heartbeat_value=0, **kwargs):
        _RedisWorkerConfig.__init__(self, conf_path)
        _Worker.__init__(self, conf_path, **kwargs)
        self.heartbeat_value = heartbeat_value
        self.redis_man.set(self.heartbeat_key, heartbeat_value)
        self._msg_manager = None

    def has_heartbeat(self):
        current_value = self.redis_man.get(self.heartbeat_key)
        if current_value != self.heartbeat_value:
            self.worker_log("heartbeat is %s, now is %s" % (self.heartbeat_value, current_value))
            return False
        return True

    def pop_task(self):
        next_task = self.redis_man.blpop(self.queue_key, self.pop_time_out)
        if next_task is not None:
            return next_task[1]
        return next_task

    def push_task(self, task_info):
        self.redis_man.rpush(self.queue_key, task_info)

    def worker_log(self, msg, level="INFO"):
        if self.log_dir is None:
            return
        if level != "INFO":
            self.publish_message(msg)
        log_file = os.path.join(self.log_dir, "%s.log" % self.work_tag)
        now_time = datetime.now().strftime(TIME_FORMAT)
        with open(log_file, "a", 0) as wl:
            wl.write("%s: %s %s\n" % (now_time, level, msg))

    def task_log(self, msg, level="INFO"):
        if self.current_task is None:
            return
        if level != "INFO":
            self.publish_message("%s\n%s" % (self.current_task, msg))
        log_file = os.path.join(self.log_dir, "%s_%s.log" % (self.work_tag, self.current_task))
        now_time = datetime.now().strftime(TIME_FORMAT)
        with open(log_file, "a", 0) as wl:
            wl.write("%s: %s %s\n" % (now_time, level, msg))

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

    def parse_task_info(self, task_info):
        partition_task = task_info.split(",", 4)
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
        args = partition_task[3]
        if partition_task[2] == "json":
            try:
                args = json.loads(args)
            except ValueError:
                error_msg = "Invalid task %s, task args type and args not uniform" % task_info
                return False, error_msg
        return True, [key, args]

    def run(self):
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
            self.current_task = task_args[0]
            self.worker_log("Start Execute %s" % self.current_task)
            self.execute(task_args[0], task_args[1])
            self.worker_log("Completed Task %s" % self.current_task)


if __name__ == "__main__":
    r_worker = RedisWorker()
    r_worker.run()
