#! /usr/bin/env python
# coding: utf-8

import os
import re
import json
import types
from JYTools.util.string import SimpleString
from JYTools import StringTool
from _exception import WorkerTaskParamsKeyNotFound, WorkerTaskParamsValueTypeError

__author__ = '鹛桑够'


class TaskStatus(object):
    """
        add in version 0.1.19
    """
    NONE = SimpleString("None")  # 任务未被设置状态或状态未知
    SUCCESS = SimpleString("Success")  # 任务执行成功
    FAIL = SimpleString("Fail")  # 任务执行失败 完全等同于ERROR
    ERROR = SimpleString("Fail")  # 任务执行失败 完全等同于FAIL
    INVALID = SimpleString("Invalid")  # 任务无效，缺少必须要的参数，或者参数不符合要求
    READY = SimpleString("Ready")  # 任务已具备运行的条件，等待执行，可能在排队或者硬件资源不足
    RUNNING = SimpleString("Running")  # 任务正在运行
    STOPPING = SimpleString("Stopping")  # 任务接收到终止信号 正在终止中
    STOPPED = SimpleString("Stopped")  # 任务接收到终止信号 已经终止

    @staticmethod
    def is_success(status):
        if status is None:
            return False
        return TaskStatus.SUCCESS.lower() == status.lower()

    @staticmethod
    def is_running(status):
        if status is None:
            return False
        return TaskStatus.RUNNING.lower() == status.lower()

    @staticmethod
    def is_fail(status):
        if status is None:
            return False
        return TaskStatus.FAIL.lower() == status.lower()

    @staticmethod
    def is_ready(status):
        if status is None:
            return False
        if status.lower() == TaskStatus.READY.lower():
            return True
        return False

    @staticmethod
    def is_none(status):
        if status is None:
            return True
        if status.lower() == TaskStatus.NONE.lower():
            return True
        return False

    @classmethod
    def parse(cls, s):
        if isinstance(s, TaskStatus):
            return s
        if StringTool.is_string(s) is False:
            return None
        for key, value in cls.__dict__.items():
            if StringTool.is_string(value) is False:
                continue
            if key.startswith("_") is True:
                continue
            if isinstance(value, SimpleString):
                continue
            if value.lower() == s.lower():
                return value
        return None


class TaskType(object):

    Normal = 1
    Report = 2
    Control = 3


class WorkerTaskParams(dict):
    """
        add in version 0.5.0
    """

    def __init__(self, seq=None, **kwargs):
        if seq is not None:
            super(WorkerTaskParams, self).__init__(seq, **kwargs)
        else:
            super(WorkerTaskParams, self).__init__(**kwargs)
        self.debug_func = None

    def get(self, k, d=None):
        v = dict.get(self, k, d)
        if isinstance(self.debug_func, types.MethodType) is True:
            self.debug_func(k, v)
        return v

    def getint(self, k, d=None):
        """
        add in version 0.7.10
        """
        v = self.get(k, d)
        if isinstance(v, int) is False:
            raise WorkerTaskParamsValueTypeError(k, v, int)
        return v

    def getboolean(self, k, d=None):
        """
        add in version 0.7.10
        """
        v = self.get(k, d)
        if isinstance(v, bool) is False:
            raise WorkerTaskParamsValueTypeError(k, v, bool)
        return v

    def getlist(self, k, d=None):
        """
        add in version 0.7.11
        """
        v = self.get(k, d)
        if isinstance(v, list) is False:
            raise WorkerTaskParamsValueTypeError(k, v, list)
        return v

    def getpath(self, k, d=None):
        """
        add in version 1.4.5
        """
        v = self.get(k, d)
        v = StringTool.encode(v)
        if os.path.exists(v) is False:
            raise WorkerTaskParamsValueTypeError(k, v, "path")
        return v

    def __getitem__(self, item):
        if item not in self:
            raise WorkerTaskParamsKeyNotFound(item)
        v = dict.__getitem__(self, item)
        if isinstance(self.debug_func, types.MethodType) is True:
            self.debug_func(item, v)
        return v


class WorkerTask(object):
    """
        add in version 0.1.19
        task_name add in version 0.2.6
    """
    __slots__ = ("task_key", "task_name", "task_sub_key", "task_info", "task_params", "task_status", "task_report_tag",
                 "is_report_task", "task_output", "task_message", "task_errors", "work_tag", "start_time", "end_time",
                 "sub_task_detail", "log_path", "task_report_scene", "task_type")

    def __init__(self, **kwargs):
        self.task_type = TaskType.Normal
        self.task_key = None
        self.task_name = None
        self.task_sub_key = None
        self.task_info = None
        self.task_params = None
        self.task_status = TaskStatus.NONE
        self.task_report_tag = None  # 任务结束后汇报的的work_tag
        self.task_report_scene = 2  # 仅任务结束后汇报
        self.is_report_task = False
        self.task_output = dict()
        self.task_message = None  # 保存任务的执行结果的综述
        self.task_errors = []  # 保存多条错误记录
        self.work_tag = None
        self.start_time = None  # 任务真正执行的开始时间
        self.end_time = None  # 任务真正执行结束的时间
        self.sub_task_detail = None
        self.log_path = None  # add in 1.1.8
        self.set(**kwargs)

    def _set_report_tag(self, report_tag):
        if report_tag is None:
            self.task_report_tag = None
            return
        m_r = re.match("^([^:]+):(\d+)$", report_tag)
        if m_r is not None:
            self.task_report_tag = m_r.groups()[0]
            self.task_report_scene = int(m_r.groups()[1])
        else:
            self.task_report_tag = report_tag

    def set(self, **kwargs):
        allow_keys = ["task_key", "task_status", "task_name", "sub_task_detail", "task_sub_key", "task_info",
                      "task_params", "task_report_tag", "is_report_task", "work_tag", "task_message", "start_time",
                      "end_time", "task_output", "task_errors", "task_type"]
        for k, v in kwargs.items():
            if k not in allow_keys:
                continue
            if k == "task_report_tag":
                self._set_report_tag(v)
                continue
            if k == "is_report_task":
                if v is True:
                    self.task_type = TaskType.Report
            self.__setattr__(k, v)

    def to_dict(self):
        d = dict()
        d["task_key"] = self.task_key
        d["task_sub_key"] = self.task_sub_key
        # d["task_info"] = self.task_info
        # d["task_params"] = self.task_params
        d["task_name"] = self.task_name
        d["task_status"] = self.task_status
        d["task_output"] = self.task_output
        d["work_tag"] = self.work_tag
        d["task_message"] = self.task_message
        d["task_errors"] = self.task_errors
        d["start_time"] = self.start_time
        d["end_time"] = self.end_time
        d["sub_task_detail"] = self.sub_task_detail
        return d

    def __getitem__(self, item):
        return self.to_dict()[item]

    def __contains__(self, item):
        return item in self.to_dict()

    def __setitem__(self, key, value):
        kwargs = {key: value}
        self.set(**kwargs)

    def __eq__(self, other):
        if isinstance(other, WorkerTask) is False:
            return False
        if other.task_key != self.task_key:
            return False
        if other.task_sub_key != self.task_sub_key:
            return False
        return True

    def add_error_msg(self, *args):
        for arg in args:
            self.task_errors.append(arg)

    def __str__(self):
        return json.dumps(self.to_dict())

if __name__ == "__main__":
    print(json.dumps({"s": SimpleString("s")}))
    print(SimpleString("Abc") == "abc")
    StringTool.is_string(SimpleString("ddd"))
    ts = TaskStatus.parse("Stopping2")
    print(ts)
    print(TaskStatus.RUNNING)
    print(TaskStatus.RUNNING == "Running")
    print(TaskStatus.RUNNING == "RUNNING")
    print(TaskStatus.RUNNING == "Running2")
    print("Running" == TaskStatus.RUNNING)

    print(TaskStatus.ERROR == TaskStatus.RUNNING)
    print(TaskStatus.RUNNING == TaskStatus.RUNNING.value)
    print(TaskStatus.is_running("ABD"))
    import json
    print(json.dumps({"expected_status": TaskStatus.STOPPED}))

