#! /usr/bin/env python
# coding: utf-8

import os
import sys
import types
import ConfigParser
from time import time, sleep
from datetime import datetime
import json
import traceback
from redis import Redis
from JYTools import TIME_FORMAT
from JYTools import StringTool
from _Exception import TaskErrorException, InvalidTaskException

__author__ = 'meisanggou'


class TaskStatus(object):
    """
        add in version 0.1.19
    """
    NONE = "None"
    SUCCESS = "Success"
    FAIL = "Fail"
    INVALID = "Invalid"
    RUNNING = "Running"


class WorkerTask(object):
    """
        add in version 0.1.19
    """

    def __init__(self, **kwargs):
        self.task_key = None
        self.task_sub_key = None
        self.task_info = None
        self.task_params = None
        self.task_status = TaskStatus.NONE
        self.task_report_tag = None  # 任务结束后汇报的的work_tag
        self.is_report_task = False
        self.set(**kwargs)

    def set(self, **kwargs):
        if "task_key" in kwargs:
            self.task_key = kwargs["task_key"]
        if "task_sub_key" in kwargs:
            self.task_sub_key = kwargs["task_sub_key"]
        if "task_info" in kwargs:
            self.task_info = kwargs["task_info"]
        if "task_params" in kwargs:
            self.task_params = kwargs["task_params"]
        if "task_report_tag" in kwargs:
            self.task_report_tag = kwargs["task_report_tag"]
        if "is_report_task" in kwargs:
            self.is_report_task = kwargs["is_report_task"]


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


class RedisQueue(_RedisWorkerConfig, _WorkerConfig):
    def __init__(self, conf_path, **kwargs):
        _RedisWorkerConfig.__init__(self, conf_path)
        _WorkerConfig.__init__(self, conf_path, **kwargs)

    @staticmethod
    def package_task_info(work_tag, key, args, sub_key=None, report_tag=None):
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
        if isinstance(args, dict):
            v += "json," + json.dumps(args)
        else:
            v += "string," + args
        return v

    def push_head(self, key, params, work_tag=None):
        v = self.package_task_info(self.work_tag, key, params)
        self.redis_man.lpush(self.queue_key, v)

    def push_tail(self, key, params, work_tag=None, sub_key=None, report_tag=None):
        v = self.package_task_info(self.work_tag, key, params, sub_key=sub_key, report_tag=report_tag)
        self.redis_man.rpush(self.queue_key, v)

    def push(self, key, params, work_tag=None, sub_key=None, report_tag=None):
        self.push_tail(key, params, work_tag, sub_key=sub_key, report_tag=report_tag)


class RedisWorker(_RedisWorkerConfig, _Worker):
    """
        expect_params_type
        add in version 0.1.8
    """
    expect_params_type = None

    def __init__(self, conf_path=None, heartbeat_value="0", **kwargs):
        self.conf_path = conf_path
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

    def push_task(self, key, params, work_tag=None):
        if work_tag is None:
            queue_key = self.queue_key
        else:
            queue_key = self.queue_prefix_key + "_" + work_tag
        task_info = RedisQueue.package_task_info(work_tag, key, params)
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
        task_item = WorkerTask(task_info=task_info)

        partition_task = task_info.split(",", 3)
        if len(partition_task) != 4:
            error_msg = "Invalid task %s, task partition length is not 3" % task_info
            return False, error_msg

        work_tags = partition_task[0].split("|")  # 0 work tag 1 return tag
        if work_tags[0] != self.work_tag:
            error_msg = "Invalid task %s, task not match work tag %s" % (task_info, self.work_tag)
            return False, error_msg
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
