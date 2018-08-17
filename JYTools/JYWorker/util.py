#! /usr/bin/env python
# coding: utf-8
__author__ = '鹛桑够'


valid_tag = ""


class ReportScene(object):

    BEGIN = 1
    Begin = 1
    END = 2
    End = 2

    @classmethod
    def include_begin(cls, scene):
        if scene & cls.Begin == cls.Begin:
            return True
        return False

    @classmethod
    def include_end(cls, scene):
        if scene & cls.End == cls.End:
            return True
        return False
