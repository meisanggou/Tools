#! /usr/bin/env python
# coding: utf-8

import sys
import getopt
from JYTools.JYWorker import DAGWorker, receive_argv

__author__ = 'meisanggou'

help_message = """

"""

if __name__ == "__main__":
    long_opts = ["help", "worker-conf-path=", "heartbeat-value=", "work-tag=", "log-dir=", "daemon", "mns-conf-dir="]
    opts, o_args = getopt.gnu_getopt(sys.argv[1:], "hc:b:w:l:Dm:", long_opts)
    opts_d = dict()
    for opt_item in opts:
        opts_d[opt_item[0]] = opt_item[1]
    if "-h" in opts_d or "--help" in opts_d:
        print(help_message)
        exit()
    conf_path = receive_argv(opts_d, "c", "worker-conf-path", None)
    heartbeat_value = receive_argv(opts_d, "b", "heartbeat-value", None)
    work_tag = receive_argv(opts_d, "w", "work-tag", "Pipeline")
    log_dir = receive_argv(opts_d, "l", "log-dir", None)
    daemon = receive_argv(opts_d, "D", "daemon", False)
    app = DAGWorker(conf_path=conf_path, heartbeat_value=heartbeat_value, work_tag=work_tag, log_dir=log_dir)
    try:
        from JYAliYun.AliYunAccount import RAMAccount
        from JYAliYun.AliYunMNS.AliMNSServer import MNSServerManager
        mns_conf_path = receive_argv(opts_d, "m", "mns-conf-path", None)
        if mns_conf_path is not None:
            mns_account = RAMAccount(conf_path=mns_conf_path)
            mns_server = MNSServerManager(mns_account, conf_path=mns_conf_path)
            mns_topic = mns_server.get_topic("JYWaring")
            app.msg_manager = mns_topic
    except Exception as e:
        print(e)
    if daemon is not False:
        app.work(daemon=True)
    else:
        app.work()
