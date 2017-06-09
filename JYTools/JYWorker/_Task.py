#! /usr/bin/env python
# coding: utf-8
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
        self.task_output = dict()
        self.work_tag = None
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
        if "work_tag" in kwargs:
            self.work_tag = kwargs["work_tag"]

    def to_dict(self):
        d = dict()
        d["task_key"] = self.task_key
        d["task_sub_key"] = self.task_sub_key
        d["task_info"] = self.task_info
        d["task_params"] = self.task_params
        d["task_status"] = self.task_status
        d["task_output"] = self.task_output
        d["work_tag"] = self.work_tag
        return d
