#! /usr/bin/env python
# coding: utf-8

import os
import sys
import json
import types
import subprocess
import uuid
from time import time, sleep
import threading
import traceback
from ._exception import TaskErrorException, InvalidTaskException, WorkerTaskParamsKeyNotFound
from ._Task import TaskStatus, WorkerTask, WorkerTaskParams
from ._config import WorkerConfig, WorkerLogConfig

__author__ = 'meisanggou'


class _WorkerLog(WorkerLogConfig):
    def worker_log(self, *args, **kwargs):
        pass

    def task_log(self, *args, **kwargs):
        pass

    """
    add in 0.7.5
    """
    def task_debug_log(self, *args, **kwargs):
        kwargs.update(level="DEBUG")
        self.task_log(*args, **kwargs)


class Worker(WorkerConfig, _WorkerLog):

    """
        expect_params_type
        add in version 0.6.9
    """
    expect_params_type = None

    def __init__(self, log_dir=None, work_tag=None, **kwargs):
        WorkerConfig.__init__(self, work_tag=work_tag, **kwargs)
        _WorkerLog.__init__(self, log_dir=log_dir, **kwargs)
        self._msg_manager = None
        self.is_running = False
        self._debug = False
        self.before_handler_funcs = []
        self.after_handler_funcs = []
        self.init_log_dir()
        self._handle_task_func = self.handle_task
        self.num_success_job = 0  # add in 0.8.1
        self.num_fail_job = 0  # add in 0.8.1
        self.num_wrongful_job = 0  # add in 0.8.1
        self.num_invalid_job = 0  # add in 0.8.1
        self.num_null_job = 0  # add in 0.8.1

    """
    add in 0.4.0
    """
    def init_log_dir(self):
        if self.log_dir is not None:
            exclusive_log_dir = os.path.join(self.log_dir, self.work_tag.lower())
            if os.path.isdir(exclusive_log_dir):
                self.log_dir = exclusive_log_dir
            else:
                try:
                    os.mkdir(exclusive_log_dir)
                    self.log_dir = exclusive_log_dir
                except OSError:
                    pass

    """
    property
    add in 0.6.9
    """

    @property
    def debug(self):
        return self._debug

    @debug.setter
    def debug(self, v):
        if self.is_running is True:
            return
        if not isinstance(v, bool):
            raise TypeError("need bool value for debug")
        self._debug = v
        if self.debug is True:
            self.redirect_stdout = False

    @property
    def num_total_job(self):
        r_job = self.num_worked_job
        t_job = r_job + self.num_wrongful_job + self.num_null_job
        return t_job

    @property
    def num_worked_job(self):
        return self.num_success_job + self.num_fail_job + self.num_invalid_job

    def has_heartbeat(self):
        return True

    def write(self, *args, **kwargs):
        self.task_log(*args, **kwargs)

    def push_task(self, key, params, work_tag=None, sub_key=None, is_report=False):
        pass

    @staticmethod
    def _subprocess_timeout_thread(p, timeout):
        """
        add in version 0.7.7
        :param p:
        :param timeout:
        :return:
        """
        while timeout > 0:
            r_code = p.poll()
            if r_code is not None:
                return
            timeout -= 1
            sleep(1)
        p.kill()
        return

    def execute_subprocess(self, cmd, stdout=None, stderr=None, error_continue=False, timeout=None):
        self.task_debug_log(cmd)
        if isinstance(cmd, list) is True:
            cmd = map(lambda x: str(x) if isinstance(x, int) else x, cmd)
        std_out = stdout
        std_err = stderr
        if std_out is None:
            std_out = subprocess.PIPE
        if std_err is None:
            if std_out == subprocess.PIPE:
                std_err = subprocess.STDOUT
            else:
                std_err = subprocess.PIPE
        child = subprocess.Popen(cmd, stderr=std_err, stdout=std_out)
        if isinstance(timeout, int) and timeout > 0:
            t_timeout = threading.Thread(target=self._subprocess_timeout_thread, args=(child, timeout))
            t_timeout.start()
        else:
            t_timeout = None

        if child.stdout is not None:
            std_log = child.stdout
        elif child.stderr is not None:
            std_log = child.stderr
        else:
            std_log = None
        exec_msg = ""
        while std_log:
            out_line = std_log.readline()
            if out_line is None or len(out_line) <= 0:
                break
            exec_msg += out_line
            self.task_log(out_line)
        child.wait()
        if t_timeout is not None:
            t_timeout.join()
        r_code = child.returncode
        if r_code != 0:
            if error_continue is False:
                self.set_current_task_error(cmd[0], " exit code not 0, is ", r_code)
            else:
                self.task_debug_log(cmd[0], " exit code not 0, is ", r_code, " but continue return.")
        else:
            self.task_debug_log(cmd[0], " exit code 0")
        return r_code, exec_msg

    def _execute(self):
        self.worker_log("Start Execute", self.current_task.task_key)
        self.hang_up_clock(1)
        self.current_task.start_time = time()
        standard_out = None
        try:
            for func in self.before_handler_funcs:
                func()
            if self.redirect_stdout is True:
                standard_out = sys.stdout
                sys.stdout = self
            self.current_task.task_status = TaskStatus.RUNNING
            if self.current_task.is_report_task is False:
                self._handle_task_func(self.current_task.task_key, self.current_task.task_params)
            else:
                self.handle_report_task()
            self.current_task.task_status = TaskStatus.SUCCESS
            if standard_out is not None:
                sys.stdout = standard_out
            for func in reversed(self.after_handler_funcs):
                func()
            self.num_success_job += 1
        except WorkerTaskParamsKeyNotFound as pk:
            self.current_task.task_status = TaskStatus.FAIL
            self.current_task.task_message = "Need Key %s, Not Found." % pk.missing_key
            self.task_log(self.current_task.task_message, level="ERROR")
            self.num_invalid_job += 1
        except TaskErrorException as te:
            self.current_task.task_status = TaskStatus.FAIL
            self.current_task.task_message = te.error_message
            self.worker_log("Task: ", te.key, "Params: ", te.params, " Error Info: ", te.error_message, level="ERROR")
            self.task_log(te.error_message, level="ERROR")
            self.num_fail_job += 1
        except InvalidTaskException as it:
            self.current_task.task_status = TaskStatus.INVALID
            self.current_task.task_message = it.invalid_message
            self.task_log(it.invalid_message, level="WARING")
            self.worker_log("Invalid Task ", it.task_info, " Invalid Info: ", it.invalid_message, level="WARING")
            self.num_invalid_job += 1
        except Exception as e:
            self.current_task.task_status = TaskStatus.FAIL
            self.current_task.task_message = str(e)
            self.task_log(traceback.format_exc(), level="ERROR")
            self._execute_error(e)
            self.num_fail_job += 1
        except SystemExit as se:
            self.current_task.task_status = TaskStatus.FAIL
            self.current_task.task_message = str(se)
            self.task_log(traceback.format_exc(), level="ERROR")
            self.num_fail_job += 1
        finally:
            if standard_out is not None:
                sys.stdout = standard_out
            self.current_task.end_time = time()
            if self.current_task.is_report_task is False and self.current_task.task_report_tag is not None:
                self.task_log("Start Report Task Status")
                self.push_task(self.current_task.task_key, self.current_task.to_dict(),
                               work_tag=self.current_task.task_report_tag, sub_key=self.current_task.task_sub_key,
                               is_report=True)
        use_time = self.current_task.end_time - self.current_task.start_time
        self.task_log("Use ", use_time, " Seconds")
        self.worker_log("Completed Task", self.current_task.task_key)
        task_output = self.current_task.task_output
        task_status = self.current_task.task_status
        self.current_task = None
        return task_status, task_output

    def _execute_error(self, e):
        if self.handler_task_exception is not None:
            self.handler_task_exception(e)

    # 待废弃 被handle_task替代
    def handler_task(self, key, params):
        pass

    # 子类需重载的方法
    def handle_task(self, key, params):
        self.handler_task(key, params)

    # 待废弃 被handle_report_task替代
    def handler_report_task(self):
        """
            add in version 0.1.19
        """
        pass

    def handle_report_task(self):
        self.handler_report_task()

    # 子类需重载的方法
    def handler_task_exception(self, e):
        pass

    def handle_invalid_task(self, task_info, error_info):
        pass

    def hang_up_clock(self, freq=None):
        pass

    def hang_down_clock(self):
        pass

    def set_current_task_invalid(self, *args):
        """
            add in version 0.1.14
        """
        if self.current_task.task_key is not None:
            raise InvalidTaskException(self.current_task.task_key, self.current_task.task_params, self.current_task,
                                       *args)

    def set_current_task_error(self, *args):
        """
            add in version 0.1.18
        """
        if self.current_task.task_key is not None:
            raise TaskErrorException(self.current_task.task_key, self.current_task.task_params, *args)

    def set_output(self, key, value):
        self.task_log("Task Out ", key, ": ", value)
        if isinstance(self.current_task, WorkerTask):
            self.current_task.task_output[key] = value

    def set_multi_output(self, **kwargs):
        for key, value in kwargs.items():
            self.set_output(key, value)

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

    def test(self, key, params=None, params_path=None, sub_key=None, report_tag=None, debug=True):
        self.debug = debug
        if params is None and params_path is not None:
            with open(params_path, "r") as rp:
                c = rp.read()
                params = json.loads(c)
        task_item = WorkerTask(task_key=key, sub_key=sub_key, report_tag=report_tag, work_tag=self.work_tag)
        if self.expect_params_type is not None:
            if not isinstance(params, self.expect_params_type):
                raise TypeError("params should", self.expect_params_type)
        if isinstance(params, dict):
            task_item.set(task_params=WorkerTaskParams(**params))
            task_item.task_params.debug_func = self.task_debug_log
        else:
            task_item.set(task_params=params)
        self.current_task = task_item
        return self._execute()

    def work(self, daemon=False):
        """
        add in version 0.1.8
        """
        if daemon is not False:
            self.debug = False
            try:
                pid = os.fork()
                if pid == 0:  # pid大于0代表是父进程 返回的是子进程的pid pid==0为子进程
                    self.run()
            except OSError:
                sys.exit(1)
        else:
            self.run()

    def close(self, exit_code=0):
        self.is_running = False
        self.hang_down_clock()
        self.worker_log("start close. exit code: %s" % exit_code)
        exit(exit_code)
