#! /usr/bin/env python
# coding: utf-8

import os
import string
import six
import random
from JYTools.util.string import is_string

# add in version 0.1.7

__author__ = 'meisanggou'

encoding = "utf-8"
second_encoding = "gb18030"


def decode(s):
    if isinstance(s, six.binary_type):
        try:
            return s.decode(encoding)
        except UnicodeError:
            return s.decode(second_encoding, "replace")
    if isinstance(s, (int, six.integer_types)):
        return "%s" % s
    return s


def encode(s):
    if isinstance(s, six.text_type):
        return s.encode(encoding)
    return s


def join(a, join_str):
    r_a = ""
    if is_string(a):
        r_a += decode(a) + join_str
    elif isinstance(a, (tuple, list)):
        for item in a:
            r_a += join(item, join_str)
    else:
        r_a += decode(str(a)) + join_str
    return r_a


def join_decode(a, join_str=""):
    join_str = decode(join_str)
    if is_string(a):
        r_a = decode(a)
    elif isinstance(a, (tuple, list)):
        a_tmp = map(lambda x: join_decode(x, join_str), a)
        r_a = join_str.join(a_tmp)
    else:
        r_a = str(a)
    return r_a


def join_encode(a, join_str=""):
    # join_str = encode(join_str)
    # if is_string(a):
    #     r_a = encode(a)
    # elif isinstance(a, (tuple, list)):
    #     a_tmp = map(lambda x: join_encode(x, join_str), a)
    #     r_a = join_str.join(a_tmp)
    # else:
    #     r_a = str(a)
    r_a = encode(join_decode(a, join_str=join_str))
    return r_a


def path_join(path, *paths):
    path = encode(path)
    ps = []
    for p in paths:
        ps.append(encode(p))
    return os.path.join(path, *ps)


def m_print(s):
    s = encode(s)
    print(s)


def random_str(str_len=32, upper_s=False):
    """ 随机生成str_len位字符串
    @return: str_len位字符串
    """
    rule = string.ascii_letters + string.digits
    c_list = random.sample(rule, str_len)
    s = "".join(c_list)
    if upper_s is True:
        return s.upper()
    return s


if __name__ == "__main__":
    l_s =["1", "abcd", "鹛桑够", ["ff", "gg", "ee"], "u鹛桑够", u"华人".encode("GB2312"), u" 중국 사람 ".encode("GB18030")]
    je = join_encode(l_s, u"中")
    print(type(je))
    print(je)
    je = join_decode(l_s, u"中")
    print(type(je))
    print(je)