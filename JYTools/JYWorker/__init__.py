#! /usr/bin/env python
# coding: utf-8

import os
import types
from ._redis import RedisWorker, RedisQueue, RedisStat
from ._async import AsyncRedisWorker, AsyncStatRedisWorker
from ._Task import TaskStatus
from ._du import DAGWorker

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


def _sub_class(b_class, c_class):
    """
    add in 1.0.1
    :param b_class:
    :param c_class:
    :return:
    """
    if issubclass(b_class, c_class) is True:
        return b_class
    if issubclass(c_class, b_class) is True:
        return c_class
    return None


def min_class(classes):
    """
    add in 1.0.1
    :param classes:
    :return:
    """
    if isinstance(classes, list) is False:
        return []
    right_classes = []
    for c_item in classes:
        if isinstance(c_item, types.TypeType) is False:
            continue
        if issubclass(c_item, RedisWorker):
            right_classes.append(c_item)
    if len(right_classes) <= 0:
        return []

    m_s = [right_classes[0]]
    for i in range(1, len(right_classes)):
        for j in range(len(m_s)):
            t_class = _sub_class(m_s[j], right_classes[i])
            if t_class is None:
                m_s.append(right_classes[i])
            else:
                m_s[j] = t_class
    return m_s
