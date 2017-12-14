#! /usr/bin/env python
# coding: utf-8

import os
import tempfile
import ConfigParser
from _Task import WorkerTask
from redis import Redis

__author__ = 'meisanggou'


class WorkerConfig(object):
    """
        [Worker]
        heartbeat_prefix_key: worker_heartbeat
        queue_prefix_key: task_queue
        work_tag: jy_task
        pop_time_out: 60
    """

    DEFAULT_WORK_TAG = None

    def __init__(self, conf_path=None, section_name="Worker", work_tag=None, is_queue=False, **kwargs):
        self.heartbeat_prefix_key = "worker_heartbeat"
        self.worker_index = None
        self.queue_prefix_key = "task_queue"
        self.pop_time_out = 60
        self.redirect_stdout = False
        if conf_path is not None:
            self.load_work_config(conf_path, section_name)
        if work_tag is not None:
            self.work_tag = work_tag
        else:
            self.work_tag = self.DEFAULT_WORK_TAG
        if isinstance(self.work_tag, (unicode, str)) is False and is_queue is False:
            class_name = self.__class__.__name__
            msg = "Need String work_tag. Please Set {0}.DEFAULT_WORK_TAG=yourWorkTag Or {0}(work_tag=yourWorkTag)"
            raise TypeError(msg.format(class_name))
        if "worker_index" in kwargs:
            self.worker_index = kwargs["worker_index"]
        if "redirect_stdout" in kwargs:
            self.redirect_stdout = kwargs["redirect_stdout"]
        if is_queue is False:
            self.heartbeat_key = self.heartbeat_prefix_key + "_" + self.work_tag
            self.queue_key = self.queue_prefix_key + "_" + self.work_tag
            if self.heartbeat_key == self.queue_key:
                self.heartbeat_key = "heartbeat_" + self.heartbeat_key
        self.current_task = WorkerTask()

    def load_work_config(self, conf_path, section_name):
        config = ConfigParser.ConfigParser()
        config.read(conf_path)
        if config.has_section(section_name):
            if config.has_option(section_name, "heartbeat_prefix_key"):
                self.heartbeat_prefix_key = config.get(section_name, "heartbeat_prefix_key")
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


class WorkerLogConfig(object):

    log_dir_environ_key = "JY_WORKER_LOG_DIR"

    def __init__(self, log_dir=None, no_logging=False, **kwargs):
        self.log_dir = None
        if log_dir is not None:
            self.log_dir = log_dir
            print("User %s as log directory" % self.log_dir)
        elif os.environ.get(self.log_dir_environ_key) is not None:
            self.log_dir = os.environ.get(self.log_dir_environ_key)
            print("Use %s as log directory. from env %s" % (self.log_dir, self.log_dir_environ_key))
        else:
            self.log_dir = tempfile.gettempdir()
            print("Use temp dir %s as log directory" % self.log_dir)
        if no_logging is True:
            self.log_dir = None
            print("Not Allow logging")


class RedisWorkerConfig(object):
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
