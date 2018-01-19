#! /usr/bin/env python
# coding: utf-8

from JYTools import StringTool

__author__ = 'meisanggou'

"""
add in version 0.1.18
"""


class InvalidTaskException(Exception):
    def __init__(self, key=None, params=None, task_info=None, *args):
        self.key = key
        self.params = params
        self.task_info = task_info
        self.invalid_message = StringTool.join(args, "")


class TaskErrorException(Exception):
    def __init__(self, key, params, *args):
        self.key = key
        self.params = params
        self.error_message = StringTool.join(args, "")


class InvalidTaskKey(Exception):

    def __str__(self):
        return "Task Key Length Must Be Greater Than 0"


class InvalidWorkerTag(Exception):

    def __str__(self):
        return "Worker Tag Must Be String And Length Greater Than 0"


class WorkerTaskParamsKeyNotFound(Exception):

    def __init__(self, key):
        self.missing_key = key

    def __str__(self):
        return "Not Found Key %s" % self.missing_key


class WorkerTaskParamsValueTypeError(Exception):

    def __init__(self, k, v, t):
        self.key = k
        self.value = v
        self.except_type = t
