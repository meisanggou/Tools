#! /usr/bin/env python
# coding: utf-8

import six


__author__ = '鹛桑够'


def is_string(s):
    if isinstance(s, (six.binary_type, six.text_type)) is False:
        return False
    return True


class SimpleString(unicode):

    def __eq__(self, other):
        if is_string(other) is False:
            return False
        return self.lower() == other.lower()

    @property
    def value(self):
        return self
