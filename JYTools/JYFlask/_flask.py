#! /usr/bin/env python
# coding: utf-8

import sys
from datetime import datetime
import re
from flask import Flask, jsonify

__author__ = 'meisanggou'


#  内置JYFlask 增加 app_url_prefix即所有注册路由的前缀 添加APP运行时间 run_time 自动注册handle500
class _JYFlask(Flask):
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, import_name, **kwargs):
        self.app_url_prefix = kwargs.pop('url_prefix', "").rstrip("/")
        self.run_time = datetime.now().strftime(self.TIME_FORMAT)
        self._broken_rules = set()
        super(_JYFlask, self).__init__(import_name, **kwargs)
        self.register_error_handler(500, self._handle_500)

    def add_broken_rule(self, rule):
        if isinstance(rule, (str, unicode)):
            self._broken_rules.add(rule)

    def clear_broken_rules(self):
        self._broken_rules.clear()

    def remove_broken_rule(self, rule):
        self._broken_rules.remove(rule)

    def list_broken_rules(self):
        return list(self._broken_rules)

    def run(self, host=None, port=None, debug=None, **options):
        self.run_time = datetime.now().strftime(self.TIME_FORMAT)
        if port is not None and port <= 0:
            sys.stderr.write("Not run. port must greater than 0.\n")
            return None
        super(_JYFlask, self).run(host=host, port=port, debug=debug, **options)

    def add_url_rule(self, rule, endpoint=None, view_func=None, **options):
        for item in self._broken_rules:
            if re.search(item, rule) is not None:
                sys.stderr.write("Not add %s, is broken rule\n" % rule)
                return None
        rule = self.app_url_prefix + rule
        super(_JYFlask, self).add_url_rule(rule=rule, endpoint=endpoint, view_func=view_func, **options)

    def _handle_500(self, e):
        resp = jsonify({"status": self.config.get("ERROR_STATUS", 99), "message": str(e)})
        return resp


class MFlask(_JYFlask):

    @staticmethod
    def help():
        print("----------MFlask HELP----------")
