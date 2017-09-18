#! /usr/bin/env python
# coding: utf-8

import string
import random

# add in version 0.1.7

__author__ = 'meisanggou'

encoding = "utf-8"
second_encoding = "gbk"


def decode(s):
    if isinstance(s, str):
        try:
            return s.decode(encoding)
        except UnicodeError:
            return s.decode(second_encoding, "replace")
    if isinstance(s, (int, long)):
        return "%s" % s
    return s


def encode(s):
    if isinstance(s, unicode):
        return s.encode(encoding)
    return s


def join(a, join_str):
    r_a = ""
    if isinstance(a, (unicode, str)):
        r_a += decode(a) + join_str
    elif isinstance(a, (tuple, list)):
        for item in a:
            r_a += join(item, join_str)
    else:
        r_a += decode(str(a)) + join_str
    return r_a


def random_str(str_len=32, upper_s=False):
    """ 随机生成str_len位字符串
    @return: str_len位字符串
    """
    rule = string.letters + string.digits
    c_list = random.sample(rule, str_len)
    s = "".join(c_list)
    if upper_s is True:
        return s.upper()
    return s


def is_string(s):
    if isinstance(s, (str, unicode)) is False:
        return False
    return True
