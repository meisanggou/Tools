#! /usr/bin/env python
# coding: utf-8

import six
import json


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


class StringEscape(object):

    def __init__(self, transfer_c="\\", spec_chars=None):
        self.transfer_c = transfer_c
        self.spec_chars_d = {transfer_c: transfer_c}
        if isinstance(spec_chars, (list, tuple)):
            for item in spec_chars:
                if len(item) != 1:
                    raise ValueError("spec_chars every char must len 1")
                if item in self.spec_chars_d.keys():
                    raise ValueError("spec_chars not allow same data and not allow same as transfer_c")
                self.spec_chars_d[item] = item
        elif isinstance(spec_chars, dict):
            v_s = set(transfer_c)
            for key, value in spec_chars.items():
                if len(key) != 1:
                    raise ValueError("spec_chars every key must len 1")
                if len(value) != 1:
                    raise ValueError("spec_chars every value must len 1")
                if key in self.spec_chars_d.keys():
                    raise ValueError("spec_chars key not allow same as transfer_c")
                if key in v_s:
                    raise ValueError("spec_chars key not allow same as any spec_chars value")
                if value in v_s:
                    raise ValueError("spec_chars value not allow same")
                v_s.add(value)
                self.spec_chars_d[key] = value

    def escape(self, s):
        if is_string(s):
            es = ""
            index = 0
            while index < len(s):
                if s[index] in self.spec_chars_d.keys():
                    es += self.transfer_c + self.spec_chars_d[s[index]]
                    index += 1
                else:
                    es += s[index]
                    index += 1
            return es
        return s

    def unescape(self, s):
        if is_string(s):
            us = ""
            index = 0
            while index < len(s):
                if s[index] != self.transfer_c:
                    us += s[index]
                    index += 1
                    continue
                find = False
                for key, value in self.spec_chars_d.items():
                    if s[index + 1] == value:
                        us += key
                        index += 2
                        find = True
                        break
                if find is False:
                    us += s[index]
                    index += 1
            return us
        return s


class StringData(object):
    BOOL_VALUE = [False, True]

    @staticmethod
    def package_data(data):
        if data is None:
            return "n_"
        if isinstance(data, dict):
            return "d_" + json.dumps(data)
        if isinstance(data, list):
            return "l_" + json.dumps(data)
        if isinstance(data, bool):
            return "b_%s" % StringData.BOOL_VALUE.index(data)
        if isinstance(data, six.integer_types):
            return "i_%s" % data
        if isinstance(data, float):
            return "f_%s" % data
        else:
            return "s_%s" % data

    @staticmethod
    def unpack_data(p_data):
        if is_string(p_data) is False:
            return p_data
        sp_data = p_data.split("_", 1)
        if len(sp_data) != 2:
            return p_data
        sign = sp_data[0]
        if sign == "s":
            return sp_data[1]
        if sign == "d":
            return json.loads(sp_data[1])
        elif sign == "l":
            return json.loads(sp_data[1])
        elif sign == "i":
            return int(sp_data[1])
        elif sign == "f":
            return float(sp_data[1])
        elif sign == "b":
            return StringData.BOOL_VALUE[int(sp_data[1])]
        elif sign == "n":
            return None
        return


if __name__ == "__main__":
    origin_str = "abc_MS_END_1\nabc|MS|END|1\nabc-MS_END-1"
    se1 = StringEscape(spec_chars=["_", "|"])
    str1 = se1.escape(origin_str)
    print(str1)
    u_str1 = se1.unescape(str1)
    if u_str1 != origin_str:
        raise ValueError()
    se2 = StringEscape("^", spec_chars={"_": "-", "|": "+"})
    str2 = se2.escape(origin_str)
    print(str2)
    u_str2 = se2.unescape(str2)
    print(u_str2)
    if u_str2 != origin_str:
        raise ValueError()
    u_str2_ = se2.unescape(origin_str)
    if u_str2_ != origin_str:
        raise ValueError()
