#! /usr/bin/env python
# coding: utf-8
import os
import logging
import tempfile
try:
    import ConfigParser as configparser
except ImportError:
    import configparser
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
    __slots__ = ("heartbeat_prefix_key", "worker_index", "queue_prefix_key", "clock_prefix_key", "pop_time_out",
                 "_id", "redirect_stdout", "work_tag", "heartbeat_key", "queue_key", "clock_key", "current_task")

    def __init__(self, conf_path=None, section_name="Worker", work_tag=None, **kwargs):
        self.heartbeat_prefix_key = "worker_heartbeat"
        self.worker_index = None
        self.queue_prefix_key = "task_queue"
        self.clock_prefix_key = "CK"
        self.pop_time_out = 60
        self.redirect_stdout = False
        if conf_path is not None:
            self.load_work_config(conf_path, section_name)
        if work_tag is not None:
            self.work_tag = work_tag
        else:
            self.work_tag = self.DEFAULT_WORK_TAG
        self.resolve_conflict()

    def resolve_conflict(self):
        h_c = q_c = c_c = False
        if self._prefix_key_conflict(self.heartbeat_prefix_key, self.queue_prefix_key) is True:
            h_c = q_c = True
        if self._prefix_key_conflict(self.heartbeat_prefix_key, self.clock_prefix_key) is True:
            h_c = c_c = True
        if self._prefix_key_conflict(self.clock_prefix_key, self.queue_prefix_key) is True:
            c_c = q_c = True
        if h_c is True:
            self.heartbeat_prefix_key = "HB_" + self.heartbeat_prefix_key
        if q_c is True:
            self.queue_prefix_key = "QL_" + self.queue_prefix_key
        if c_c is True:
            self.clock_prefix_key = "CK_" + self.clock_prefix_key

    @staticmethod
    def _prefix_key_conflict(key1, key2):
        if key1.find(key2) == 0:
            return True
        if key2.find(key1) == 0:
            return True
        return False

    def load_work_config(self, conf_path, section_name):
        config = configparser.ConfigParser()
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

    @staticmethod
    def write_work_config(file_path, heartbeat_prefix_key=None, queue_prefix_key=None, pop_time_out=None,
                          section_name="Worker", append=True):
        """
        Add in version 0.8.7
        :param file_path:
        :param heartbeat_prefix_key:
        :param queue_prefix_key:
        :param pop_time_out:
        :param section_name:
        :param append:
        :return:
        """
        mode = "w"
        if append is True:
            mode = "a"
        c = "[%s]\n" % section_name
        if heartbeat_prefix_key is not None:
            c += "heartbeat_prefix_key: %s\n" % heartbeat_prefix_key
        if queue_prefix_key is not None:
            c += "queue_prefix_key: %s\n" % queue_prefix_key
        if pop_time_out is not None:
            c += "pop_time_out: %s\n" % pop_time_out
        with open(file_path, mode) as wf:
            wf.write(c)
        return True, c


class WorkerLogConfig(object):

    log_dir_environ_key = "JY_WORKER_LOG_DIR"

    def __init__(self, log_dir=None, no_logging=False, **kwargs):
        self.log_dir = None
        if log_dir is not None:
            self.log_dir = log_dir
            logging.debug("User %s as log directory" % self.log_dir)
        elif os.environ.get(self.log_dir_environ_key) is not None:
            self.log_dir = os.environ.get(self.log_dir_environ_key)
            logging.debug("Use %s as log directory. from env %s" % (self.log_dir, self.log_dir_environ_key))
        else:
            self.log_dir = tempfile.gettempdir()
            logging.debug("Use temp dir %s as log directory" % self.log_dir)
        if no_logging is True:
            self.log_dir = None
            logging.debug("Not Allow logging")
        self.origin_log_dir = self.log_dir


class RedisWorkerConfig(object):
    """
        [Redis]
        redis_host: localhost
        redis_port: 6379
        redis_password:
        redis_db: 13
    """

    def __init__(self, conf_path=None, redis_host=None, redis_password=None, redis_port=None, redis_db=None,
                 section_name="Redis", redis_man=None):
        self.redis_host = "localhost"
        self.redis_port = 6379
        self.redis_password = None
        self.redis_db = 13
        if conf_path is not None:
            self.load_redis_config(conf_path, section_name)
        if self.redis_password == "":
            self.redis_password = None

        if redis_host is not None:
            self.redis_host = redis_host
        if redis_password is not None:
            self.redis_password = redis_password
        if redis_port is not None:
            self.redis_port = redis_port
        if redis_db is not None:
            self.redis_db = redis_db

        self.redis_man = redis_man
        if self.redis_man is None:
            self.connected = False
            self._connect()
        else:
            self.connected = True

    def _connect(self):
        self.redis_man = Redis(host=self.redis_host, port=self.redis_port, db=self.redis_db,
                               password=self.redis_password)
        self.connected = True

    def load_redis_config(self, conf_path, section_name):
        config = configparser.ConfigParser()
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

    @staticmethod
    def write_config(file_path, redis_host=None, redis_password=None, redis_port=None, redis_db=None,
                     section_name="Redis", append=True):
        """
        Add in version 0.7.11
        :param file_path:
        :param redis_host:
        :param redis_password:
        :param redis_port:
        :param redis_db:
        :param section_name:
        :param append:
        :return:
        """
        mode = "w"
        if append is True:
            mode = "a"
        c = "[%s]\n" % section_name
        if redis_host is not None:
            c += "redis_host: %s\n" % redis_host
        if redis_password is not None:
            c += "redis_password: %s\n" % redis_password
        if redis_port is not None:
            c += "redis_port: %s\n" % redis_port
        if redis_db is not None:
            c += "redis_db: %s\n" % redis_db
        with open(file_path, mode) as wf:
            wf.write(c)
        return True, c


if __name__ == "__main__":
    RedisWorkerConfig.write_config("redis.conf", redis_host="localhost", redis_password="", redis_port=6379, redis_db=1)