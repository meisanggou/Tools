#! /usr/bin/env python
# coding: utf-8

import sys
import ConfigParser
from time import time
import traceback
from redis import Redis
from _Exception import TaskErrorException, InvalidTaskException
from _Task import TaskStatus

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
        self.redirect_stdout = False
        if conf_path is not None:
            self.load_work_config(conf_path, section_name)
        if "work_tag" in kwargs:
            self.work_tag = kwargs["work_tag"]
        if "worker_index" in kwargs:
            self.worker_index = kwargs["worker_index"]
        if "redirect_stdout" in kwargs:
            self.redirect_stdout = kwargs["redirect_stdout"]
        self.heartbeat_key = self.heartbeat_prefix_key + "_" + self.work_tag
        self.queue_key = self.queue_prefix_key + "_" + self.work_tag
        if self.heartbeat_key == self.queue_key:
            self.heartbeat_key = "heartbeat_" + self.heartbeat_key
        self.current_task = None

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
        pass

    def task_log(self, *args, **kwargs):
        pass


class _Worker(_WorkerConfig, _WorkerLog):
    def __init__(self, **kwargs):
        _WorkerConfig.__init__(self, **kwargs)
        _WorkerLog.__init__(self, **kwargs)

    def has_heartbeat(self):
        return True

    def write(self, *args, **kwargs):
        self.task_log(*args, **kwargs)

    def push_task(self, key, params, work_tag=None, is_report=False):
        pass

    def execute(self):
        execute_time = time()
        standard_out = None
        try:
            if self.redirect_stdout is True:
                standard_out = sys.stdout
                sys.stdout = self
            self.current_task.task_status = TaskStatus.RUNNING
            if self.current_task.is_report_task is False:
                self.handler_task(self.current_task.task_key, self.current_task.task_params)
            else:
                self.handler_report_task()
            self.current_task.task_status = TaskStatus.SUCCESS
            if standard_out is not None:
                sys.stdout = standard_out
        except TaskErrorException as te:
            self.current_task.task_status = TaskStatus.FAIL
            self.worker_log("Task: ", te.key, "Params: ", te.params, " Error Info: ", te.error_message, level="ERROR")
            self.task_log(te.error_message, level="ERROR")
        except InvalidTaskException as it:
            self.current_task.task_status = TaskStatus.INVALID
            self.worker_log("Invalid Task ", it.task_info, " Invalid Info: ", it.invalid_message, level="WARING")
        except Exception as e:
            self.current_task.task_status = TaskStatus.FAIL
            self.task_log(traceback.format_exc(), level="ERROR")
            self.execute_error(e)
        finally:
            if standard_out is not None:
                sys.stdout = standard_out
            if self.current_task.is_report_task is False and self.current_task.task_report_tag is not None:
                self.task_log("Start Report Task Status")
                self.push_task(self.current_task.task_key, self.current_task,
                               work_tag=self.current_task.task_report_tag, is_report=True)
        use_time = time() - execute_time
        self.task_log("Use ", use_time, " Seconds")

    def execute_error(self, e):
        if self.handler_task_exception is not None:
            self.handler_task_exception(e)

    # 子类需重载的方法
    def handler_task(self, key, params):
        pass

    def handler_report_task(self):
        """
            add in version 0.1.19
        """
        pass

    # 子类需重载的方法
    def handler_task_exception(self, e):
        pass

    def handler_invalid_task(self, task_info, error_info):
        pass

    def set_current_task_invalid(self, *args):
        """
            add in version 0.1.14
        """
        if self.current_task is not None:
            raise InvalidTaskException(self.current_task.task_key, self.current_task.task_params, self.current_task,
                                       *args)

    def set_current_task_error(self, *args):
        """
            add in version 0.1.18
        """
        if self.current_task is not None:
            raise TaskErrorException(self.current_task.task_key, self.current_task.task_params, *args)

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
