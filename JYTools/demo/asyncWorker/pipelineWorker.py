#! /usr/bin/env python
# coding: utf-8


from JYTools.JYWorker import DAGWorker

__author__ = 'meisanggou'


p_w = DAGWorker(conf_path="redis_worker.conf", heartbeat_value="FFFFFF", log_dir="/tmp", work_tag="Pipeline",
                stat_work_tag="STAT")
p_w.work()
