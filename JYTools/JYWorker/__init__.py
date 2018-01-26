#! /usr/bin/env python
# coding: utf-8

import os
from _redis import RedisWorker, RedisQueue, RedisStat
from _async import AsyncRedisWorker, AsyncStatRedisWorker
from _Task import TaskStatus
from _du import DAGWorker

__author__ = 'meisanggou'


def _print():
    print()


def receive_argv(d, short_opt, long_opt, default_value="", **kwargs):
    short_opt_key = "-" + short_opt
    long_opt_key = "--" + long_opt
    if short_opt_key in d:
        print("use %s %s" % (short_opt_key, d[short_opt_key]))
        return d[short_opt_key]
    if long_opt_key in d:
        print("use %s %s" % (long_opt_key, d[long_opt_key]))
        return d[long_opt_key]
    env_key = long_opt.replace("-", "_").upper()
    env_value = os.environ.get(long_opt.replace("-", "_").upper())
    if env_value is not None:
        print("use env value %s: %s" % (env_key, env_value))
        return env_value
    if default_value == "":
        exit("please use %s or %s" % (short_opt_key, long_opt_key))
    return default_value
