#! /usr/bin/env python
# coding: utf-8

import sys
import uuid
import tempfile
import getopt
from JYAliYun.AliYunAccount import RAMAccount
from JYAliYun.AliYunMNS.AliMNSServer import MNSServerManager
from JYTools.JYWorker import receive_argv, DAGWorker

__author__ = 'meisanggou'


help_message = """
optional arguments:
    -h, --help show this help message and exit
    -c STRING, --conf-path worker conf path. the path must exist. [redis_worker.conf]
    -b STRING, --heartbeat-value STRING heartbeat value. [uuid.uuid4().hex]
    -w STRING, --work-tag STRING work tag. [Pipeline]
    -l STRING, --log-dir worker log save dir. [os tempdir]
    -D, --daemon Daemonize the Pipeline Worker. [False]
    -m STRING, --mns-conf-path STRING mns conf path
    -d STRING, --db-conf-path STRING db conf path
"""

if __name__ == "__main__":
    long_opts = ["help", "conf-path=", "heartbeat-value=", "work-tag=", "log-dir=", "daemon", "mns-conf-dir=",
                 "db-conf-path="]
    opts, o_args = getopt.gnu_getopt(sys.argv[1:], "hc:b:w:l:Dm:d:", long_opts)
    opts_d = dict()
    for opt_item in opts:
        opts_d[opt_item[0]] = opt_item[1]
    if "-h" in opts_d or "--help" in opts_d:
        print(help_message)
        exit()
    conf_path = receive_argv(opts_d, "c", "conf-path", "redis_worker.conf")
    heartbeat_value = receive_argv(opts_d, "b", "heartbeat-value", uuid.uuid4().hex)
    work_tag = receive_argv(opts_d, "w", "work-tag", "Pipeline")
    log_dir = receive_argv(opts_d, "l", "log-dir", tempfile.gettempdir())
    daemon = receive_argv(opts_d, "D", "daemon", False)
    app = DAGWorker(conf_path=conf_path, heartbeat_value=heartbeat_value, work_tag=work_tag, log_dir=log_dir)
    mns_conf_path = receive_argv(opts_d, "m", "mns-conf-path")
    mns_account = RAMAccount(conf_path=mns_conf_path)
    mns_server = MNSServerManager(mns_account, conf_path=mns_conf_path)
    mns_topic = mns_server.get_topic("JYWaring")
    app.msg_manager = mns_topic
    if daemon is not False:
        app.work(daemon=True)
    else:
        app.work()
