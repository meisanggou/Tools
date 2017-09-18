#! /usr/bin/env python
# coding: utf-8

from datetime import datetime
from flask import Flask, jsonify

__author__ = 'meisanggou'


#  内置JYFlask 增加 app_url_prefix即所有注册路由的前缀 添加APP运行时间 run_time 自动注册handle500
class _JYFlask(Flask):
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

    def __init__(self, import_name, **kwargs):
        self.app_url_prefix = kwargs.pop('url_prefix', "").rstrip("/")
        self.run_time = datetime.now().strftime(self.TIME_FORMAT)
        super(_JYFlask, self).__init__(import_name, **kwargs)
        self.register_error_handler(500, self._handle_500)

    def run(self, host=None, port=None, debug=None, **options):
        self.run_time = datetime.now().strftime(self.TIME_FORMAT)
        super(_JYFlask, self).run(host=host, port=port, debug=debug, **options)

    def add_url_rule(self, rule, endpoint=None, view_func=None, **options):
        rule = self.app_url_prefix + rule
        super(_JYFlask, self).add_url_rule(rule=rule, endpoint=endpoint, view_func=view_func, **options)

    def _handle_500(self, e):
        resp = jsonify({"status": self.config.get("ERROR_STATUS", 99), "message": str(e)})
        return resp
