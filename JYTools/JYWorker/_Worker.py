#! /usr/bin/env python
# coding: utf-8

import os
import sys
import types
from time import time
import traceback
from _exception import TaskErrorException, InvalidTaskException
from _Task import TaskStatus
from _config import WorkerConfig, WorkerLogConfig

__author__ = 'meisanggou'


class _WorkerLog(WorkerLogConfig):
    def worker_log(self, *args, **kwargs):
        pass

    def task_log(self, *args, **kwargs):
        pass


class Worker(WorkerConfig, _WorkerLog):
    def __init__(self, **kwargs):
        WorkerConfig.__init__(self, **kwargs)
        _WorkerLog.__init__(self, **kwargs)
        self._msg_manager = None

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
                self.push_task(self.current_task.task_key, self.current_task.to_dict(),
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

    def run(self):
        pass

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

    def close(self, exit_code=0):
        self.worker_log("start close. exit code: %s" % exit_code)
        exit(exit_code)
