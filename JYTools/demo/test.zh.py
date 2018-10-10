#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import RedisQueue, TaskStatus

__author__ = 'meisanggou'

r_queue = RedisQueue("/mnt/data/Tools/JYTools/demo/redis_worker.conf", work_tag="Pipeline")


plus_task = {"work_tag": "Plus", "input_a": "&0a", "input_b": "&0abc*", "task_name": "Plusa+0"}
plus_10_task = {"work_tag": "Plus", "input_a": "&1c", "input_b": 10}

mult_10_task = {"work_tag": "Mult", "input_a": "&1c", "input_b": 10}

mult_task = {"work_tag": "Mult", "input_a": "&2c", "input_b": "&3c"}

plus_100_task = {"work_tag": "Plus", "input_a": "&0a", "input_b": 100}
mult_100_task = {"work_tag": "Mult", "input_a": "&0b", "input_b": 100}
plus_m_task = {"work_tag": "Plus", "input_a": "&5c", "input_b": "&6c"}

mult_m_task = {"work_tag": "Mult", "input_a": "&7c", "input_b": "&4c"}

# a=2 b=3
# (((a + b) + 10) * ((a + b) * 10)) * ((a + 100) + (b * 100))
# ((5 + 10) * (5 * 10)) * (102 + 300)
# (15 * 50) * 402
# 750 + 402
# 301500

pipeline_detail = {"input_a": "&0a", "input_b": "&0b", "task_list": [plus_task, plus_10_task, mult_10_task, mult_task,
                                                                     plus_100_task, mult_100_task, plus_m_task,
                                                                     mult_m_task]}
# pipeline_detail["task_output"] = {"d": "&8c"}
pipeline_detail["output_d"] = "&8c"
pipeline_detail["task_type"] = "pipeline"
# pipeline_detail["work_tag"] = "Pipeline"


mult_20_task = {"work_tag": "Mult", "input_a": "&1d", "input_b": 20}

merge_task = {"work_tag": "Merge", "input_v": ["&1d", "&2c", 1]}
repeat_pipeline_detail2 = {"task_list": [pipeline_detail, mult_20_task, merge_task], "output_lc": "&3m",
                           "task_type": "repeat-pipeline"}
# repeat_pipeline_detail2["work_tag"] = "Pipeline"
repeat_pipeline_detail2["repeat_freq"] = 5
repeat_pipeline_detail2["input_a"] = 2
repeat_pipeline_detail2["input_b"] = 3

repeat_plus_task = {"work_tag": "Plus", "input_a": [], "input_b": 10, "task_type": "repeat-app",
                    "task_output": {"lc": "&c"}, "task_name": "+10"}
for i in range(10):
    repeat_plus_task["input_a"].append(i)

merge_task = {"work_tag": "Merge", "input_v": "&1lc"}
pipeline_detail3 = {"task_list": [repeat_pipeline_detail2, merge_task], "task_output": {"y": "&2m", "y2": "&2r"}}
# r_queue.push("cccc", pipeline_detail3, report_tag="Result")



split_vcf = {"work_tag": "SplitVCF", "input_chrome": "&0chrome", "input_vcf_path": ["a.vcf", "b.vcf", "c.vcf"],
             "task_type": "repeat-app",
             "task_output": {"o": "&out_path"}}
combine = {"input_vcfs": "&1o", "input_chrome": "&0chrome", "work_tag": "Combine", "task_type": "app"}
pipeline_split = {"input_chrome": "&0chr", "task_list": [split_vcf, combine], "task_type": "repeat-pipeline",
                  "task_output": {"o": "&1o", "m": "&2m"}}

pipeline_detail4 = {"task_list": [pipeline_split], "task_output": {"m": "&1m"}, "input_chr": ["chr1", "chr2", "chr3"]}
# r_queue.push("vs2d", pipeline_detail4, report_tag="Result")


# for i in range(1, 10000):
#     v = []
#     for j in range(1, 10000000):
#         v.append(i + j)
#     r_queue.push(i, {"v": v})

import time
key = int(time.time()) % 100
pipeline_detail5 = {"task_list": [plus_task, repeat_plus_task], "input_a": 10, "input_abc1": 6}
print(key)
r_queue.push(key, pipeline_detail5, report_tag="Result")
# r_queue.push_control(key, "Pipeline", TaskStatus.STOPPED, force=True, report_file="/tmp/pipeline/Pipeline_%s.r.json" % key)