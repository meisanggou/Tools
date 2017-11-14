#! /usr/bin/env python
# coding: utf-8

from _exception import WorkerTaskParamsKeyNotFound

__author__ = 'meisanggou'


class TaskStatus(object):
    """
        add in version 0.1.19
    """
    NONE = "None"
    SUCCESS = "Success"
    FAIL = "Fail"
    ERROR = "Fail"
    INVALID = "Invalid"
    RUNNING = "Running"


class WorkerTaskParams(dict):
    """
        add in version 0.5.0
    """

    def __getitem__(self, item):
        if item not in self:
            raise WorkerTaskParamsKeyNotFound(item)
        return dict.__getitem__(self, item)


class WorkerTask(object):
    """
        add in version 0.1.19
        task_name add in version 0.2.6
    """

    def __init__(self, **kwargs):
        self.task_key = None
        self.task_name = None
        self.task_sub_key = None
        self.task_info = None
        self.task_params = None
        self.task_status = TaskStatus.NONE
        self.task_report_tag = None  # 任务结束后汇报的的work_tag
        self.is_report_task = False
        self.task_output = dict()
        self.task_message = None
        self.work_tag = None
        self.start_time = None
        self.end_time = None
        self.sub_task_detail = None
        self.set(**kwargs)

    def set(self, **kwargs):
        if "task_key" in kwargs:
            self.task_key = kwargs["task_key"]
        if "task_status" in kwargs:
            self.task_status = kwargs["task_status"]
        if "task_name" in kwargs:
            self.task_name = kwargs["task_name"]
        if "sub_task_detail" in kwargs:
            self.sub_task_detail = kwargs["sub_task_detail"]
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
        if "task_message" in kwargs:
            self.task_message = kwargs["task_message"]
        if "start_time" in kwargs:
            self.start_time = kwargs["start_time"]
        if "end_time" in kwargs:
            self.end_time = kwargs["end_time"]
        if "task_output" in kwargs:
            self.task_output = kwargs["task_output"]

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


if __name__ == "__main__":
    wp = WorkerTaskParams(a=1, b=2)
    for key in wp:
        print(key)
    print wp.keys()
    print(wp["a"])
    print(wp["c"])