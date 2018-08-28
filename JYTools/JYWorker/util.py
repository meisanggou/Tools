#! /usr/bin/env python
# coding: utf-8
import re

__author__ = '鹛桑够'


class ReportScene(object):

    BEGIN = 1
    Begin = 1
    END = 2
    End = 2
    RealTime = 4

    @classmethod
    def include_begin(cls, scene):
        if scene is None:
            return False
        if scene & cls.Begin == cls.Begin:
            return True
        return False

    @classmethod
    def include_end(cls, scene):
        if scene is None:
            return False
        if scene & cls.End == cls.End:
            return True
        return False

    @classmethod
    def include_real_time(cls, scene):
        if scene is None:
            return False
        if scene & cls.RealTime == cls.RealTime:
            return True
        return False


class ValueVerify(object):
    comp_tag = re.compile(r"^[\w\-]+$", re.I)
    comp_heartbeat = re.compile(r"^[\da-zA-Z]{3,50}$", re.I)

    @classmethod
    def v_work_tag(cls, work_tag):
        if cls.comp_tag.match(work_tag) is None:
            return False
        return True

    @classmethod
    def v_report_tag(cls, report_tag):
        return cls.v_work_tag(work_tag=report_tag)

    @classmethod
    def v_heartbeat(cls, heartbeat):
        if cls.comp_heartbeat.match(heartbeat) is None:
            return False
        return True
