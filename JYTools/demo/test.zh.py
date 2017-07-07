#! /usr/bin/env python
# coding: utf-8

from JYTools.JYWorker import RedisQueue

__author__ = 'meisanggou'

r_queue = RedisQueue("/home/msg/Tools/JYTools/demo/redis_worker.conf", work_tag="Pipeline")
print(r_queue.queue_key)

plus_task = {"work_tag": "Plus", "input_a": "&0a", "input_b": "&0b"}
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
                    "task_output": {"lc": "&c"}}
for i in range(10):
    repeat_plus_task["input_a"].append(i)

merge_task = {"work_tag": "Merge", "input_v": "&1lc"}
pipeline_detail3 = {"task_list": [repeat_pipeline_detail2, merge_task], "task_output": {"y": "&2m"}}
r_queue.push("cccc", pipeline_detail3, report_tag="Result")
