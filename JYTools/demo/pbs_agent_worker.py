#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import RedisWorker


__author__ = '鹛桑够'


class PBSAgentWorker(RedisWorker):

    expect_params_type = dict

    def handle_task(self, key, params):
        sub_key = self.current_task.task_sub_key
        report_tag = self.current_task.task_report_tag
        self.current_task.task_report_tag = None
        work_tag = params["work_tag"]
        n_params = params["params"]
        self.push_task(key, n_params, work_tag=work_tag, sub_key=sub_key, report_tag=report_tag)


if __name__ == "__main__":
    args = PBSAgentWorker.parse_args()
    if args.work_tag is None:
        args.work_tag = "PBSAgent"
    app = PBSAgentWorker(conf_path=args.conf_path, heartbeat_value=args.heartbeat_value, work_tag=args.work_tag,
                         log_dir=args.log_dir)
    if args.daemon is not None:
        app.work(daemon=True)
    else:
        app.work()