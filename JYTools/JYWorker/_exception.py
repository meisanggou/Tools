#! /usr/bin/env python
# coding: utf-8


__author__ = 'meisanggou'

"""
add in version 0.1.18
"""


class InvalidTaskException(Exception):
    def __init__(self, key=None, params=None, task_info=None, *args):
        self.key = key
        self.params = params
        self.task_info = task_info
        self.invalid_message = args


class TaskErrorException(Exception):
    def __init__(self, key, params, *args):
        self.key = key
        self.params = params
        self.error_message = args
