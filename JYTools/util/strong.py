#! /usr/bin/env python
# coding: utf-8

import random
import syslog
from JYTools.util.string_rich import StringData
from redis import Redis, RedisError

__author__ = '鹛桑够'


class StrongRedis(Redis):

    def execute_command(self, *args, **options):
        try:
            return Redis.execute_command(self, *args, **options)
        except RedisError as r_error:
            syslog.syslog(*args)
            syslog.syslog(r_error)


class CacheRedis(Redis):

    def __init__(self, key_prefix=None, error_sleep=1000, host='localhost', port=6379, db=0, password=None,
                 socket_connect_timeout=1):
        self.key_prefix = key_prefix
        if key_prefix is not None:
            Redis.__init__(self, host, port, db, password=password, socket_connect_timeout=socket_connect_timeout)
        self.error_num = 0
        self.error_sleep = error_sleep

    def execute_command(self, *args, **options):
        if 0 < self.error_num < self.error_sleep:
            self.error_num += 1
            return None
        try:
            return Redis.execute_command(self, *args, **options)
        except RedisError as r_error:
            self.error_num = 1
            syslog.syslog(str(r_error))
            return None

    def get(self, name):
        if self.key_prefix is None:
            return None
        name = self.key_prefix + name
        v = Redis.get(self, name)
        v = StringData.unpack_data(v)
        return v

    def set(self, name, value, ex=None, px=None, nx=False, xx=False):
        if self.key_prefix is None:
            return None
        value = StringData.package_data(value)
        name = self.key_prefix + name
        return Redis.set(self, name, value, ex=ex, px=px, nx=nx, xx=xx)

    def setnx(self, name, value):
        if self.key_prefix is None:
            return None
        name = self.key_prefix + name
        value = StringData.package_data(value)
        return Redis.setnx(self, name, value)

    def setex(self, name, value, time):
        if self.key_prefix is None:
            return None
        value = StringData.package_data(value)
        name = self.key_prefix + name
        return Redis.setex(self, name, value, time)

    def setex2(self, name, value, basic_time=60, random_ranges=None):
        if random_ranges is None:
            random_ranges = basic_time
        time = basic_time + random.randint(0, random_ranges)
        return self.setex(name, value, time)

    def delete(self, *names):
        if self.key_prefix is None:
            return None
        names = map(lambda x: self.key_prefix + x, names)
        return Redis.delete(self, *names)

if __name__ == "__main__":
    redis_man = CacheRedis("jy_api", host="172.16.110.5", port=9532, db=1)
    print(redis_man.set("bac", dict(s="123")))
    print(redis_man.get("bac"))
    redis_man.delete("bac")
