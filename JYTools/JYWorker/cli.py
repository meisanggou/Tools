#! /usr/bin/env python
# coding: utf-8

import os
import time
import json
import sys
import logging
import argparse
from JYTools import jy_input, StringTool, logger
from JYTools.JYWorker import RedisStat, RedisQueue, DAGTools, TaskStatus

__author__ = '鹛桑够'


arg_man = argparse.ArgumentParser()
arg_man.add_argument("--debug", dest="debug", help="debug mode, print debug msg", action="store_true", default=False)


def parse_args():
    args = arg_man.parse_args()
    if args.debug is True:
        logger.setLevel(logging.DEBUG)
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


def push_task():
    arg_man.add_argument("-c", "--config", dest="conf_path", help="configure file path")
    arg_man.add_argument("-w", "--work-tag", dest="work_tag", help="work tag", metavar="WorkTag", required=True)
    arg_man.add_argument("-k", "--key", dest="key", help="task key", metavar="KEY", required=True)
    arg_man.add_argument("-s", "--sub-key", dest="sub_key", metavar="SubKey", help="task sub key")
    arg_man.add_argument("--is-report", dest="is_report", help="is report task", action="store_true", default=False)
    arg_man.add_argument("params", help="task params")
    args = parse_args()
    rq = RedisQueue(conf_path=args.conf_path, work_tag=args.work_tag)
    rq.push(args.key, args.params, sub_key=args.sub_key, is_report=args.is_report)


def report_task():
    arg_man.add_argument("-w", "--work-tag", dest="work_tag", help="work tag", metavar="WorkTag")
    arg_man.add_argument("-c", "--config", dest="conf_path", help="configure file path")
    arg_man.add_argument("-r", "--report-tag", dest="report_tag", help="work tag", metavar="ReportTag", required=True)
    arg_man.add_argument("-k", "--key", dest="key", help="task key", metavar="KEY", required=True)
    arg_man.add_argument("-s", "--sub-key", dest="sub_key", metavar="SubKey", help="task sub key")
    arg_man.add_argument("--start-time", dest="start_time", metavar="StartTime", help="start time. timestamp")
    arg_man.add_argument("--task-status", dest="task_status", metavar="TaskStatus",
                         choices=[TaskStatus.SUCCESS, TaskStatus.FAIL, TaskStatus.RUNNING], required=True)
    arg_man.add_argument("--task-output", dest="task_output", metavar="TaskOutput")
    arg_man.add_argument("message", help="task message")
    empty_help()
    args = parse_args()
    rq = RedisQueue(conf_path=args.conf_path, work_tag=args.report_tag)
    task_output = args.task_output
    if task_output is not None:
        task_output = json.loads(task_output)
    params = dict(end_time=int(time.time()), task_key=args.key, task_sub_key=args.sub_key, start_time=int(time.time()),
                  task_status=args.task_status, task_message=args.message, work_tag=args.work_tag,
                  task_output=task_output)
    if args.start_time is not None:
        params["start_time"] = args.start_time
    rq.push(args.key, params, sub_key=args.sub_key, is_report=True)


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


def look_task_item():
    arg_man.add_argument("-w", "--work-tag", dest="work_tag", help="work tag", metavar="")
    args = parse_args()
    rs = RedisStat()
    values = []
    if args.work_tag is not None:
        values.append(args.work_tag)
    while True:
        prompt_prefix = ""
        if len(values) <= 0:
            work_tag = jy_input("Please Input Work_tag", prompt_prefix=prompt_prefix)
            work_tag = work_tag.strip()
            if work_tag.lower() in ("e", "exit"):
                sys.exit(0)
            values.append(work_tag)
            continue
        if len(values) == 1:
            prompt_prefix += "[work_tag:%s]" % values[0]
            key = jy_input("Please Input Task Key", prompt_prefix=prompt_prefix)
            key = key.strip()
            if key.lower() in ("e", "exit"):
                values.remove(values[-1])
                continue
            values.append(key)
            continue
        elif len(values) >= 2:
            sub_key = None
            prompt_prefix += "[work_tag:%s][key:%s]" % (values[0], values[1])
            if len(values) >= 3:
                sub_key = StringTool.join_decode(values[2:], "_")
                prompt_prefix += "[sub_key:%s]" % sub_key
            task_items = rs.list_task_item(values[0], values[1], sub_key)
            prompt = ""
            if len(task_items["sub"].keys()) > 0:
                prompt += "Input follow value look sub item\n"
                prompt += "\n".join(task_items["sub"])
                prompt += "\n"
            if len(task_items["values"].keys()) > 0:
                prompt += "Input follow value look item value\n"
                prompt += "\n".join(task_items["values"])
            while True:
                item_key = jy_input(prompt, prompt_prefix=prompt_prefix)
                item_key = item_key.strip()
                if item_key.lower() in ("e", "exit"):
                    values.remove(values[-1])
                    break
                if item_key in task_items["sub"]:
                    values.append(item_key)
                    break
                elif item_key in task_items["values"]:
                    print(task_items["values"][item_key])
                    continue
                else:
                    continue


def verify_pipeline():
    arg_man.add_argument("file", help="pipeline file", metavar="file")
    empty_help()
    args = parse_args()
    p_file = args.file
    if os.path.isfile(p_file) is False:
        logger.error("file %s not exist" % p_file)
        sys.exit(1)
    with open(p_file) as rp:
        c = rp.read()
    try:
        c_o = json.loads(c)
    except ValueError:
        logger.error("The content of the file is not legal, not the json content")
        sys.exit(1)
    r, data = DAGTools.ip_verify_pipeline(c_o)
    sys.exit(data["code"])


def clear_worker():
    rs = RedisStat()
    r_queue = RedisQueue(redis_man=rs.redis_man)
    ws = rs.list_worker()
    for item in ws:
        rs.delete_heartbeat(item)
        data = rs.list_worker_detail(item)
        num = len(data.keys())
        r_queue.wash_worker(item, num)



if __name__ == "__main__":
    # sys.argv.append("--debug")
    # sys.argv.extend(["-w", "Pipeline"])
    # wash_worker()
    sys.argv.extend(["--report-tag", "Pipeline", "-k", "100", "-s", "2", "--task-output", '{"a":"a"}', "--task-status", "Success", "success"])
    print(" ".join(sys.argv))
    # list_queue()
    report_task()