#! /usr/bin/env python
# coding: utf-8

import os
from _redis import RedisWorker, RedisQueue
from _Task import TaskStatus

__author__ = 'meisanggou'


def receive_argv(d, short_opt, long_opt, default_value=None):
    short_opt_key = "-" + short_opt
    long_opt_key = "--" + long_opt
    if short_opt_key in d:
        return d[short_opt_key]
    if long_opt_key in d:
        return d[long_opt_key]
    default_value = os.environ.get(long_opt.replace("-", "_").upper(), default_value)
    if default_value is None:
        exit("please use %s or %s" % (short_opt_key, long_opt_key))
    return default_value
