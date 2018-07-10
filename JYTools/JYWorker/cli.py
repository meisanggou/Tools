#! /usr/bin/env python
# coding: utf-8

import sys
import logging
import argparse
from JYTools.JYWorker import RedisStat, RedisQueue

__author__ = '鹛桑够'

# logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logging.basicConfig()

arg_man = argparse.ArgumentParser()
arg_man.add_argument("--debug", dest="debug", help="debug mode, print debug msg", action="store_true", default=False)


def parse_args():
    args = arg_man.parse_args()
    if args.debug is True:
        logging.basicConfig(level=logging.DEBUG, format="%(asctime)s %(levelname)s %(message)s")
    return args


def empty_help():
    if len(sys.argv) <= 1:
        sys.argv.append("-h")


def list_queue():
    arg_man.add_argument("-w", "--work-tag", dest="work_tag", help="work tag", metavar="")
    args = parse_args()
    rs = RedisStat()
    if args.work_tag is None:
        qd = rs.list_queue()
        for key in qd:
            print(key)
    else:
        lqd = rs.list_queue_detail(args.work_tag)
        for item in lqd:
            print(item)


def list_worry_queue():
    rs = RedisStat()
    wq = rs.list_worry_queue()
    for item in wq:
        print(item)


def list_heartbeat():
    arg_man.add_argument("-w", "--work-tag", dest="work_tag", help="work tag", metavar="")
    rs = RedisStat()
    args = parse_args()
    if args.work_tag is None:
        ws = rs.list_heartbeat()
        for item in ws:
            print(item)
    else:
        hd = rs.list_heartbeat_detail(args.work_tag)
        print(hd)


def delete_heartbeat():
    empty_help()
    rs = RedisStat()
    arg_man.add_argument("-w", "--work-tag", dest="work_tag", help="work tag", metavar="", nargs="*", default=[])
    args = parse_args()
    for item in args.work_tag:
        print("delete heartbeat %s" % item)
        rs.delete_heartbeat(item)


def list_worker():
    arg_man.add_argument("-w", "--work-tag", dest="work_tag", help="work tag", metavar="")
    rs = RedisStat()
    args = parse_args()
    if args.work_tag is None:
        ws = rs.list_worker()
        for item in ws:
            print(item)
    else:
        hd = rs.list_worker_detail(args.work_tag)
        print(hd)


def auto_wash_worker_func(work_tag, redis_man=None):
    # 获取当前队列中是否任务
    # 获取当前多少个Worker
    rs = RedisStat(redis_man=redis_man)
    data = rs.list_worker_detail(work_tag)
    ids = data.keys()


def wash_worker():
    empty_help()
    arg_man.add_argument("-a", "--auto", dest="auto", help="auto set num", action="store_true", default=False)
    arg_man.add_argument("-w", "--work-tag", dest="work_tag", help="work tag", metavar="", nargs="*", default=[])
    arg_man.add_argument("-n", "--num", dest="num", help="num of wash package to send", metavar="", type=int, default=1)
    args = parse_args()
    r_queue = RedisQueue()
    rs = RedisStat(redis_man=r_queue.redis_man)

    for item in args.work_tag:
        print("wash work tag %s" % item)
        if args.auto is True:
            data = rs.list_worker_detail(item)
            num = len(data.keys())
            print("In auto mode, send %s wash package %s" % (item, num))
            r_queue.wash_worker(item, num)
        else:
            r_queue.wash_worker(item, args.num)


if __name__ == "__main__":
    sys.argv.append("--debug")
    sys.argv.extend(["-a", "-w", "BatchComputer", "AnalysisScheduling2"])
    # wash_worker()
    logging.info("ssd")
    wash_worker()