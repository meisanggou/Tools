#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import DAGWorker

__author__ = 'meisanggou'


def main():
    DAGWorker.init_parser.add_argument("-a", "--agent-tag", dest="agent_tag", help="agent tag")
    DAGWorker.other_parser.add_argument("-m", "--mns-conf-path", dest="mns_conf_path", help="mns conf path")
    args = DAGWorker.parse_args()
    if args.work_tag is None:
        args.work_tag = "Pipeline"
    # args.agent_tag = "PBSAgent"
    app = DAGWorker(conf_path=args.conf_path, heartbeat_value=args.heartbeat_value, work_tag=args.work_tag,
                    log_dir=args.log_dir, agent_tag=args.agent_tag)
    try:
        from JYAliYun.AliYunAccount import RAMAccount
        from JYAliYun.AliYunMNS.AliMNSServer import MNSServerManager
        if args.mns_conf_path is not None:
            mns_account = RAMAccount(conf_path=args.mns_conf_path)
            mns_server = MNSServerManager(mns_account, conf_path=args.mns_conf_path)
            mns_topic = mns_server.get_topic("JYWaring")
            app.msg_manager = mns_topic
    except Exception as e:
        print(e)
    app.work(daemon=args.daemon)

if __name__ == "__main__":
    main()

