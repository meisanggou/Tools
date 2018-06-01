#! /usr/bin/env python
# coding: utf-8

import sys
import argparse
from JYTools.JYWorker import RedisStat, RedisQueue

__author__ = '鹛桑够'


rs = RedisStat()
r_queue = RedisQueue()


def list_queue():
    arg_man = argparse.ArgumentParser()
    arg_man.add_argument("-w", "--work-tag", dest="work_tag", help="work tag", metavar="")
    if len(sys.argv) <= 1:
        sys.argv.append("-h")
    args = arg_man.parse_args()
    if args.work_tag is None:
        rs.list_queue()
    else:
        rs.list_queue_detail(args.work_tag)


def list_worry_queue():
    rs.list_worry_queue()


def list_heartbeat():
    rs.list_heartbeat()


def wash_worker():
    arg_man = argparse.ArgumentParser()
    arg_man.add_argument("-w", "--work-tag", dest="work_tag", help="work tag", metavar="", required=True)
    arg_man.add_argument("-n", "--num", dest="num", help="num of wash package to send", metavar="", type=int, default=1)
    if len(sys.argv) <= 1:
        sys.argv.append("-h")
    args = arg_man.parse_args()
    r_queue.wash_worker(args.work_tag, args.num)