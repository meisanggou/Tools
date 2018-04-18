#! /usr/bin/env python
# coding: utf-8

import os
import time
import json
from JYTools import StringTool
from JYTools.JYWorker import RedisWorker


__author__ = '鹛桑够'


agent_tmp_dir = "/mnt/glusterfs/public/agent_tmp"
example_dir = StringTool.path_join(agent_tmp_dir, "example")
pbs_task_dir = StringTool.path_join(agent_tmp_dir, "pbs")
if os.path.isdir(example_dir) is False:
    os.mkdir(example_dir)
if os.path.isdir(pbs_task_dir) is False:
    os.mkdir(pbs_task_dir)


class PBSAgentWorker(RedisWorker):

    expect_params_type = dict

    def write_pbs_task(self, work_tag, params):
        save_name = StringTool.join_decode([self.current_task.task_key, self.current_task.task_sub_key,
                                            int(time.time()), "pbs"], join_str=".")
        save_dir = StringTool.path_join(pbs_task_dir, work_tag)
        if os.path.isdir(save_dir) is False:
            os.mkdir(save_dir)
        save_path = StringTool.path_join(save_dir, save_name)
        with open(save_path, "w") as wp:
            wp.write(StringTool.encode(json.dumps(params)))
        return save_path

    def write_example(self, work_tag, params):
        save_name = StringTool.join_decode([self.current_task.task_key, self.current_task.task_sub_key,
                                            int(time.time()), "json"], join_str=".")
        save_dir = StringTool.path_join(example_dir, work_tag)
        if os.path.isdir(save_dir) is False:
            os.mkdir(save_dir)
        save_path = StringTool.path_join(save_dir, save_name)
        with open(save_path, "w") as wp:
            wp.write(StringTool.encode(json.dumps(params)))
        return save_path

    def package_cmd(self, work_tag, report_tag, example_path):
        py_path = "/mnt/data/Tools/JYTools/demo/Plus2Worker.py"
        key = self.current_task.task_key
        cmds = ["python", py_path, "-c", self.conf_path, "-w", work_tag, "-e", example_path, "-k", key]

        sub_key = self.current_task.task_sub_key
        if sub_key is not None:
            cmds.extend(["-s", sub_key])
        if report_tag is not None:
            cmds.extend(["-r", report_tag])
        return cmds

    def handle_task(self, key, params):
        sub_key = self.current_task.task_sub_key
        report_tag = self.current_task.task_report_tag

        work_tag = params["work_tag"]
        n_params = params["params"]

        example_path = self.write_example(work_tag, n_params)
        exec_cmd = self.package_cmd(work_tag, report_tag, example_path)
        print(exec_cmd)
        self.execute_subprocess(exec_cmd)
        # self.push_task(key, n_params, work_tag=work_tag, sub_key=sub_key, report_tag=report_tag)

        self.current_task.task_report_tag = None


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