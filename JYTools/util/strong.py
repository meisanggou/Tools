#! /usr/bin/env python
# coding: utf-8

import syslog
from redis import Redis, RedisError

__author__ = '鹛桑够'


class StrongRedis(Redis):

    def execute_command(self, *args, **options):
        try:
            Redis.execute_command(self, *args, **options)
        except RedisError as r_error:
            syslog.syslog(*args)
            syslog.syslog(r_error)
