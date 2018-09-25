#! /usr/bin/env python
# coding: utf-8

import os
import re
import sys
import signal
import json
import uuid
import types
import subprocess
from time import time, sleep
import threading
import logging
import traceback
from JYTools import StringTool
from JYTools.JYWorker.util import ValueVerify, ReportScene
from ._exception import TaskErrorException, InvalidTaskException, WorkerTaskParamsKeyNotFound
from ._exception import WorkerTaskParamsValueTypeError
from ._Task import TaskStatus, WorkerTask, WorkerTaskParams, TaskType
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

        if StringTool.is_string(self.work_tag) is False:
            class_name = self.__class__.__name__
            msg = "Need String work_tag. Please Set {0}.DEFAULT_WORK_TAG=yourWorkTag Or {0}(work_tag=yourWorkTag)"
            raise TypeError(msg.format(class_name))
        if ValueVerify.v_work_tag(self.work_tag) is False:
            raise ValueError("Invalid work_tag format")
        self._id = uuid.uuid4().hex  # add in 0.9.11
        self._msg_manager = None
        self.is_running = False  # 表示worker是否已经开始运行，并不断接收任务,一旦运行起来，不可再进入test模式即调用test方法
        self._debug = False
        self.before_handle_funcs = []
        self.after_handle_funcs = []
        self.init_log_dir()
        self._handle_task_func = self.handle_task
        self.num_success_job = 0  # add in 0.8.1
        self.num_fail_job = 0  # add in 0.8.1
        self.num_wrongful_job = 0  # add in 0.8.1
        self.num_invalid_job = 0  # add in 0.8.1
        self.num_null_job = 0  # add in 0.8.1
        self.num_pop_task = 0  # add in 1.6.8 尝试去获得任务的次数（无论是否获得数据，无论从哪个队列中获得）
        if "worker_index" in kwargs:
            self.worker_index = kwargs["worker_index"]
        if "redirect_stdout" in kwargs:
            self.redirect_stdout = kwargs["redirect_stdout"]
        self.heartbeat_key = self.heartbeat_prefix_key + "_" + self.work_tag
        self.queue_key = self.queue_prefix_key + "_" + self.work_tag
        # 延时队列，该队列和普通queue相对，访问一定次数的普通queue才会访问一次该队列
        self.delay_queue_key = self.queue_prefix_key + "_" + self.work_tag + "@delay"
        self.clock_key = self.clock_prefix_key + "_" + self.work_tag + "_" + self._id
        self.current_task = WorkerTask()
        self._worker_status = 0  # 内部运行状态。目前主要用于 当收到kill信号时的处理

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
            for func in self.before_handle_funcs:
                func()
            if self.redirect_stdout is True:
                standard_out = sys.stdout
                sys.stdout = self
            self.current_task.task_status = TaskStatus.RUNNING
            if self.current_task.task_type == TaskType.Normal and self.current_task.task_report_tag is not None:
                if ReportScene.include_begin(self.current_task.task_report_scene) is True:
                    self.task_debug_log("Start Report Task Running Status")
                    self.push_task(self.current_task.task_key, self.current_task.to_dict(),
                                   work_tag=self.current_task.task_report_tag, sub_key=self.current_task.task_sub_key,
                                   is_report=True)
            if self.current_task.task_type == TaskType.Normal:
                self._handle_task_func(self.current_task.task_key, self.current_task.task_params)
            elif self.current_task.task_type == TaskType.Control:
                self.handle_control(**self.current_task.task_params)
            else:
                self.handle_report_task()
            if self.current_task.task_status == TaskStatus.RUNNING:
                self.current_task.task_status = TaskStatus.SUCCESS
            if standard_out is not None:
                sys.stdout = standard_out
            for func in reversed(self.after_handle_funcs):
                func()
            self.num_success_job += 1
        except WorkerTaskParamsKeyNotFound as pk:
            self.current_task.task_status = TaskStatus.FAIL
            self.current_task.task_message = "Need Key %s, Not Found." % pk.missing_key
            self.task_log(self.current_task.task_message, level="ERROR")
            self.num_invalid_job += 1
        except WorkerTaskParamsValueTypeError as pvt:
            self.current_task.task_status = TaskStatus.FAIL
            self.current_task.task_message = "Need Value Type %s, Not Match." % pvt.except_type
            self.task_log(self.current_task.task_message, level="ERROR")
            self.num_invalid_job += 1
        except TaskErrorException as te:
            self.current_task.task_status = TaskStatus.FAIL
            self.current_task.task_message = te.error_message
            self.worker_log("Task: ", te.key, "Params: ", te.params, " Error Info: ", te.error_message)
            self.task_log(te.error_message, level="ERROR")
            self.num_fail_job += 1
        except InvalidTaskException as it:
            self.current_task.task_status = TaskStatus.INVALID
            self.current_task.task_message = it.invalid_message
            self.task_log(it.invalid_message, level="WARING")
            self.worker_log("Invalid Task ", it.task_info, " Invalid Info: ", it.invalid_message)
            self.num_invalid_job += 1
        except Exception as e:
            if TaskStatus.is_fail(self.current_task.task_status) is True:
                #  防止重复设置FAIL和覆盖用户设置的Fail
                self.current_task.task_status = TaskStatus.FAIL
                self.current_task.task_message = str(e)
            self.task_log(traceback.format_exc(), level="ERROR")
            self._execute_error(e)
            self.num_fail_job += 1
        except SystemExit as se:
            if self.is_running is False:
                sys.exit(se.code)
            self.current_task.task_status = TaskStatus.FAIL
            self.current_task.task_message = str(se)
            self.task_log(traceback.format_exc(), level="ERROR")
            self.num_fail_job += 1
        finally:
            if standard_out is not None:
                sys.stdout = standard_out
            self.current_task.end_time = time()
            if self.current_task.task_type == TaskType.Normal and self.current_task.task_report_tag is not None:
                self.task_debug_log("Start Report Task Status")
                self.push_task(self.current_task.task_key, self.current_task.to_dict(),
                               work_tag=self.current_task.task_report_tag, sub_key=self.current_task.task_sub_key,
                               is_report=True)
        use_time = self.current_task.end_time - self.current_task.start_time
        self.task_debug_log("Use ", use_time, " Seconds")
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

    # 子类需重载的方法
    def handle_control(self, expected_status, **params):
        self.set_current_task_invalid("Worker not support control task status")

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
        self.task_debug_log("Task Out ", key, ": ", value)
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
            logging.error(e)

    def run(self, wash_old=False):
        pass

    def test(self, key, params=None, params_path=None, sub_key=None, report_tag=None, debug=True):
        if self.is_running is True:  # 一旦运行起来，不可再进入test模式即调用test方法
            raise RuntimeError("Can not test, current is running")
        self.debug = debug
        if params is None and params_path is not None:
            with open(params_path, "r") as rp:
                c = rp.read()
                params = json.loads(c)
        task_item = WorkerTask(task_key=key, task_sub_key=sub_key, task_report_tag=report_tag, work_tag=self.work_tag)
        if self.expect_params_type is not None:
            if not isinstance(params, self.expect_params_type):
                raise TypeError("params should", self.expect_params_type)
        if isinstance(params, dict):
            task_item.set(task_params=WorkerTaskParams(**params))
            task_item.task_params.debug_func = self.task_debug_log
        else:
            task_item.set(task_params=params)
        if StringTool.is_string(self.log_dir) is True:
            log_name = StringTool.join_encode([self.work_tag, "_", task_item.task_key, ".log"], join_str="")
            task_item.log_path = StringTool.path_join(self.log_dir, log_name)
        self.current_task = task_item
        return self._execute()

    def handle_sign(self, sign, frame):
        self.task_log("Worker Receive SIGN", sign)
        self.close(sign)

    def work(self, daemon=False, wash_old=True):
        """
        add in version 0.1.8
        """
        # handle SIGINT 2 from ctrl+c
        signal.signal(signal.SIGINT, self.handle_sign)
        # handle SIGTERM 15 from kill
        signal.signal(signal.SIGTERM, self.handle_sign)
        # handle
        signal.signal(signal.SIGUSR1, self.handle_sign)
        signal.signal(signal.SIGUSR2, self.handle_sign)

        if daemon is not False:
            self.debug = False
            try:
                pid = os.fork()
                if pid == 0:  # pid大于0代表是父进程 返回的是子进程的pid pid==0为子进程
                    self.run(wash_old)
            except OSError:
                sys.exit(1)
        else:
            self.run()

    def close(self, exit_code=0):
        self.is_running = False
        self.hang_down_clock()
        self.worker_log("start close. exit code: %s" % exit_code)
        sys.exit(exit_code)

"""
    ReadWorkerLog Add In Version 1.0.4
"""


class ReadWorkerLog(WorkerLogConfig):

    log_pattern = r"^\[[\s\S]+?\](\[[\s\S]*?\]|) (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}): ([a-z]{1,10}) ([\s\S]*)"
    log_compile = re.compile(log_pattern, re.I)
    log_level = dict(DEBUG=("DEBUG", "INFO", "WARING", "WARNING", "ERROR"), INFO=("INFO", "WARING", "WARNING", "ERROR"),
                     WARNING=("WARING", "WARNING", "ERROR"), ERROR=("ERROR", ))

    def read_task_log(self, work_tag, key, sub_key=None, sub_key_prefix=None, level="INFO", max_length=1000000):
        """

        :param work_tag:
        :param key:
        :param sub_key: 为None时查询所有有子key和无子key的日志，为空字符串时仅查询无子key的日志，为具体某个子key时查询具体子key的日志
        :param level: 默认为INFO，允许DEBUG，INFO，WARNING，ERROR。其他值认为是INFO
        :return:
        """
        name = StringTool.join([work_tag, "_", key, ".log"], "")
        log_path = StringTool.path_join(self.log_dir, work_tag.lower(), name)
        if os.path.exists(log_path) is False:
            log_path = StringTool.path_join(self.log_dir, name)
            if os.path.exists(log_path) is False:
                return False, None
        s_log = os.stat(log_path)
        read_seek = s_log.st_size - max_length if max_length < s_log.st_size else 0
        # 处理参数
        if sub_key is not None:
            sub_key = StringTool.encode(sub_key)
        if sub_key_prefix is not None:
            sub_key_prefix = StringTool.encode(sub_key_prefix)
        if StringTool.is_string(level) is False:
            level = "INFO"
        level = level.upper()
        if level not in self.log_level:
            level = "INFO"
        allow_levels = self.log_level[level]
        logs_list = []
        last_save = False
        with open(log_path, "r") as rl:
            rl.seek(read_seek)
            c = rl.read()
            all_lines = c.split("\n")
            for line in all_lines:
                rl = self.log_compile.match(line)
                if rl is not None:
                    line_sub_key = rl.groups()[0]
                    log_time = rl.groups()[1]
                    if len(line_sub_key) >= 2:
                        line_sub_key = line_sub_key[1:-1]
                    line_level = rl.groups()[2]
                    log_msg = rl.groups()[3]
                    if sub_key is not None and sub_key != line_sub_key:
                        last_save = False
                        continue
                    if sub_key_prefix is not None and line_sub_key.startswith(sub_key_prefix) is False:
                        last_save = False
                        continue
                    if line_level not in allow_levels:
                        last_save = False
                        continue
                    last_save = True
                    logs_list.append(map(StringTool.decode, [line_sub_key, log_time, line_level, log_msg]))
                elif last_save is True:
                    logs_list[-1][3] = StringTool.join_decode([logs_list[-1][3], line])
        return True, logs_list
