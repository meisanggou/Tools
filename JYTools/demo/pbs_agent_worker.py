#! /usr/bin/env python
# coding: utf-8

import sys
import os
import time
import json
from JYTools import StringTool
from JYTools.JYWorker import RedisWorker, worker_run


__author__ = '鹛桑够'


agent_tmp_dir = "/mnt/glusterfs/public/agent_tmp"
example_dir = StringTool.path_join(agent_tmp_dir, "example")
pbs_task_dir = StringTool.path_join(agent_tmp_dir, "pbs")
if os.path.isdir(example_dir) is False:
    os.mkdir(example_dir)
if os.path.isdir(pbs_task_dir) is False:
    os.mkdir(pbs_task_dir)

pbs_template = """#PBS -S /bin/bash
#PBS -m n
#PBS -M <zhouheng@gene.ac>
"""


class PBSAgentWorker(RedisWorker):

    expect_params_type = dict

    def write_pbs_task(self, work_tag, cmd):
        save_name = StringTool.join_decode([self.current_task.task_key, self.current_task.task_sub_key,
                                            int(time.time()), "pbs"], join_str=".")
        save_dir = StringTool.path_join(pbs_task_dir, work_tag)
        if os.path.isdir(save_dir) is False:
            os.mkdir(save_dir)
        save_path = StringTool.path_join(save_dir, save_name)
        with open(save_path, "w") as wp:
            cmd = StringTool.join_encode(cmd, join_str=" ")
            s = StringTool.join_encode([pbs_template, cmd], join_str="\n")
            wp.write(StringTool.encode(s))
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

    def package_docker_cmd(self, image, volumes):
        cmd = ["docker", "run"]
        if isinstance(volumes, dict) is True:
            for key in volumes.keys():
                cmd.extend(["-v", "%s:%s" % (key, volumes[key])])
        cmd.append(image)
        return cmd

    def package_cmd(self, work_tag, report_tag, example_path):
        py_path = "/mnt/glusterfs/public/worker/Plus2Worker.py"
        key = self.current_task.task_key
        cmd = ["python", py_path, "-c", "/mnt/glusterfs/public/conf/redis_worker.conf", "-w", work_tag, "-e", example_path, "-k", key]

        sub_key = self.current_task.task_sub_key
        if sub_key is not None:
            cmd.extend(["-s", sub_key])
        if report_tag is not None:
            cmd.extend(["-r", report_tag])
        return cmd

    def handle_task(self, key, params):
        report_tag = self.current_task.task_report_tag

        work_tag = params["work_tag"]
        n_params = params["params"]

        example_path = self.write_example(work_tag, n_params)
        exec_cmd = self.package_cmd(work_tag, report_tag, example_path)
        print(exec_cmd)
        # self.execute_subprocess(exec_cmd)
        pbs_path = self.write_pbs_task(work_tag, exec_cmd)
        self.execute_subprocess(["qsub", pbs_path])
        self.current_task.task_report_tag = None


if __name__ == "__main__":
    sys.exit(worker_run(PBSAgentWorker, default_work_tag="PBSAgent"))
