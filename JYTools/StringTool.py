#! /usr/bin/env python
# coding: utf-8

# add in version 0.1.7

__author__ = 'meisanggou'

encoding = "utf-8"


def decode(s):
    if isinstance(s, str):
        return s.decode(encoding)
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
