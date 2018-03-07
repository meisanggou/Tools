#! /usr/bin/env python
# coding: utf-8

import re
from time import time
from JYTools.StringTool import is_string
from ._Task import TaskStatus
from ._redis import RedisWorker

__author__ = 'meisanggou'


class DAGWorker(RedisWorker):
    expect_params_type = dict

    @staticmethod
    def split_ref(ref_str):
        """

        :param ref_str: index+字母开头的key index+&+数字开头的key index+&+字母开头的key
        :return:
        """
        if ref_str[0] == "&":
            ref_str = ref_str[1:]
        match_r = re.match(r"^(\d{1,10})((&\d+|&*[a-z])\w{0,60})$", ref_str, re.I)
        if match_r is None:
            return None
        ref_index = int(match_r.groups()[0])
        ref_key = match_r.groups()[1]
        if ref_key[0] == "&":
            ref_key = ref_key[1:]
        return dict(index=ref_index, key=ref_key)

    @staticmethod
    def exist_loop(params):
        tl = params["task_list"]
        assert isinstance(tl, list)
        task_len = len(tl)
        assert task_len > 0
        rs_l = [dict(quotes=list(), next=list(), index=i) for i in range(task_len)]
        for index in range(task_len):
            task_item = tl[index]
            assert isinstance(task_item, dict)
            for k, v in task_item.items():
                if k.startswith("input_") is False:
                    continue
                if is_string(v) is False:
                    continue
                ref_d = DAGWorker.split_ref(v)
                if ref_d is None:
                    continue
                if ref_d["index"] < 0 or ref_d["index"] > task_len:
                    raise ValueError("")  # out of index
                if ref_d["index"] not in rs_l[index]["quotes"]:
                    rs_l[index]["quotes"].append(ref_d["index"])
                    if ref_d["index"] > 0:
                        rs_l[ref_d["index"] - 1]["next"].append(index)
        completed_queue = [0]

        while True:
            completed_num = 0
            for index in range(task_len):
                if index + 1 in completed_queue:
                    continue
                rs_item = rs_l[index]
                q_len = len(rs_item["quotes"])
                for i in range(q_len - 1, -1, -1):
                    if rs_item["quotes"][i] in completed_queue:
                        rs_item["quotes"].remove(rs_item["quotes"][i])
                if len(rs_item["quotes"]) <= 0:
                    completed_queue.append(index + 1)
                    completed_num += 1
                    continue
            if len(completed_queue) == task_len + 1:
                return False
            if completed_num == 0:
                return True
        return False

    @staticmethod
    def find_loop2(params):
        tl = params["task_list"]
        assert isinstance(tl, list)
        task_len = len(tl)
        assert task_len > 0
        rs_l = [dict(quotes=list(), next=list(), index=i) for i in range(task_len)]
        for index in range(task_len):
            task_item = tl[index]
            assert isinstance(task_item, dict)
            for k, v in task_item.items():
                if k.startswith("input_") is False:
                    continue
                if is_string(v) is False:
                    continue
                ref_d = DAGWorker.split_ref(v)
                if ref_d is None:
                    continue
                if ref_d["index"] < 0 or ref_d["index"] > task_len:
                    raise ValueError("")  # out of index
                if ref_d["index"] not in rs_l[index]["quotes"]:
                    rs_l[index]["quotes"].append(ref_d["index"])
                    if ref_d["index"] > 0:
                        rs_l[ref_d["index"] - 1]["next"].append(index)

        for index in range(task_len):
            def link(j, l):
                if j + 1 in l:
                    l.append(j + 1)
                    return l
                if len(rs_l[j]["next"]) <= 0:
                    return None
                l.append(j + 1)
                for n_item in rs_l[j]["next"]:
                    lr_l = link(n_item, l)
                    if lr_l is not None:
                        return lr_l
                l.remove(l[-1])
                return None
            r_l = link(index, list())
            if r_l is not None:
                return r_l

        return None

    @staticmethod
    def find_loop(params):
        tl = params["task_list"]
        assert isinstance(tl, list)
        task_len = len(tl)
        assert task_len > 0
        rs_l = [dict(quotes=list(), next=list(), index=i) for i in range(task_len + 1)]
        for index in range(task_len):
            task_item = tl[index]
            assert isinstance(task_item, dict)
            for k, v in task_item.items():
                if k.startswith("input_") is False:
                    continue
                if is_string(v) is False:
                    continue
                ref_d = DAGWorker.split_ref(v)
                if ref_d is None:
                    continue
                if ref_d["index"] < 0 or ref_d["index"] > task_len:
                    raise ValueError("")  # out of index
                if ref_d["index"] not in rs_l[index + 1]["quotes"]:
                    rs_l[index + 1]["quotes"].append(ref_d["index"])
                    rs_l[ref_d["index"]]["next"].append(index + 1)

        for index in range(1, task_len):
            def link(j, l):
                if len(rs_l[j]["next"]) <= 0:
                    return None
                for n_item in rs_l[j]["next"]:
                    if n_item in l:
                        l.append(n_item)
                        return l[l.index(n_item):]
                    l.append(n_item)
                    lr_l = link(n_item, l)
                    if lr_l is not None:
                        return lr_l
                    l.remove(l[-1])
                return None
            r_l = link(index, [index])
            if r_l is not None:
                return r_l

        return None

    @staticmethod
    def find_loop3(params):
        tl = params["task_list"]
        assert isinstance(tl, list)
        task_len = len(tl)
        assert task_len > 0
        rs_l = [dict(quotes=list(), next=list(), index=i) for i in range(task_len + 1)]
        for index in range(task_len):
            task_item = tl[index]
            assert isinstance(task_item, dict)
            for k, v in task_item.items():
                if k.startswith("input_") is False:
                    continue
                if is_string(v) is False:
                    continue
                ref_d = DAGWorker.split_ref(v)
                if ref_d is None:
                    continue
                if ref_d["index"] < 0 or ref_d["index"] > task_len:
                    raise ValueError("")  # out of index
                if ref_d["index"] not in rs_l[index + 1]["quotes"]:
                    rs_l[index + 1]["quotes"].append(ref_d["index"])
                    rs_l[ref_d["index"]]["next"].append(index + 1)
        passed_point = [False] * (task_len + 1)
        for index in range(1, task_len):
            def link(j, l):
                if passed_point[j] is not False:
                    return None
                if len(rs_l[j]["next"]) <= 0:
                    passed_point[j] = True
                    return None
                for n_item in rs_l[j]["next"]:
                    if n_item in l:
                        l.append(n_item)
                        return l[l.index(n_item):]
                    l.append(n_item)
                    lr_l = link(n_item, l)
                    if lr_l is not None:
                        return lr_l
                    l.remove(l[-1])
                passed_point[j] = True
                return None
            r_l = link(index, [index])
            if r_l is not None:
                return r_l

        return None

    def handle_report_task(self):
        r_task = self.current_task.task_params
        sp_keys = self.current_task.task_sub_key.rsplit("_", 1)
        if len(sp_keys) == 2:
            self.current_task.task_sub_key = sp_keys[0]
        else:
            self.current_task.task_sub_key = None
        reporter_sub_key = int(sp_keys[-1])  # 子任务在父任务中的位置 位置从1开始
        self.task_log("Task ", reporter_sub_key, " Report")
        task_status = r_task.task_status
        task_message = r_task.task_message
        self.task_log("Task ", reporter_sub_key, " Status Is ", task_status)
        self.set_task_item(reporter_sub_key, "task_status", task_status)
        self.set_task_item(reporter_sub_key, "task_message", task_message)
        if r_task["sub_task_detail"] is not None:
            self.set_task_item(reporter_sub_key, "task_list", r_task.sub_task_detail)
        if task_status != TaskStatus.SUCCESS:
            self.set_task_item(0, "task_status", TaskStatus.FAIL)
            self.set_task_item(0, "task_message", task_message)
            self.set_task_item(0, "task_fail_index", reporter_sub_key)
            self.fail_pipeline("Sub Task Failed")
        if isinstance(r_task.task_output, dict):
            for output_key in r_task.task_output.keys():
                self.set_task_item(reporter_sub_key, "output_%s" % output_key, r_task.task_output[output_key])
        self.set_task_item(reporter_sub_key, "start_time", r_task.start_time)
        self.set_task_item(reporter_sub_key, "end_time", r_task.end_time)
        self.set_task_item(reporter_sub_key, "finished_time", time())
        self.handle_task(self.current_task.task_key, None)

    def format_pipeline(self, key, params):
        if "task_list" not in params:
            self.set_current_task_invalid("Need task_list")
        if "name" in params:
            self.set_task_item(0, "task_name", params["name"])
        task_list = params["task_list"]
        if isinstance(task_list, list) is False:
            self.set_current_task_invalid("Need tuple task_list. Now Is ", type(task_list))
        task_len = len(task_list)
        if task_len <= 0:
            self.set_current_task_invalid("At Least One Task")
        for index in range(task_len):
            self.del_task_item(index + 1)
            task_item = task_list[index]
            if isinstance(task_item, dict) is False:
                self.set_current_task_invalid("Task ", index + 1, " Desc Not Dict")
            task_type = task_item.get("task_type", "app")
            if task_type not in ("task", "pipeline", "app", "repeat-app", "repeat-pipeline"):
                self.set_current_task_invalid("Task ", index + 1, " Invalid Task Type ", task_type)
            task_item["task_type"] = task_type
            if task_type.endswith("pipeline"):
                task_item["work_tag"] = self.work_tag
            if "work_tag" not in task_item:
                self.set_current_task_invalid("Task ", index + 1, " work_tag Not Found")
            if "task_status" in task_item:
                if task_item["task_status"] != TaskStatus.SUCCESS:
                    del task_item["task_status"]
            if "task_output" in task_item:
                if isinstance(task_item["task_output"], dict):
                    for key in task_item["task_output"].keys():
                        if "output_%s" % key not in task_item:
                            task_item["output_%s" % key] = task_item["task_output"][key]
        task_output = params.get("task_output", dict())
        for key in params:
            if key.startswith("input_"):
                self.set_task_item(0, key, params[key])
            elif key.startswith("output_"):
                task_output[key[7:]] = params[key]
        self.set_task_item(0, "task_len", task_len)
        if self.current_task.task_report_tag is not None:
            self.set_task_item(0, "report_tag", self.current_task.task_report_tag)
            self.current_task.task_report_tag = None  # 真正执行完后才进行report
        self.set_task_item(0, "task_output", task_output)
        self.set_task_item(0, "start_time", time())
        for index in range(task_len):
            task_item = task_list[index]
            for key in task_item.keys():
                self.set_task_item(index + 1, key, task_item[key])

    def completed_pipeline(self):
        task_len = self.get_task_item(0, hash_key="task_len")
        task_output = self.get_task_item(0, hash_key="task_output")
        if task_output is not None:
            outputs = dict()
            for out_key in task_output.keys():
                out_value = task_output[out_key]
                if is_string(out_value) is True and out_value.startswith("&"):
                    ref_r, ref_info = self.analysis_ref(out_value[1:], None, task_len)
                    if ref_r is False:
                        continue
                    if ref_info is None:
                        continue
                    out_value = ref_info["ref_output"]
                    outputs[out_key] = out_value
                    self.set_task_item(0, "output_%s" % out_key, out_value)
                elif isinstance(out_value, list):

                    for sub_i in range(len(out_value)):
                        sub_v = out_value[sub_i]
                        if is_string(sub_v) is False or sub_v.startswith("&") is False:
                            continue
                        ref_r, ref_info = self.analysis_ref(sub_v[1:], None, task_len)
                        if ref_r is False:
                            continue
                        if ref_info is None:
                            continue
                        out_value[sub_i] = ref_info["ref_output"]
                    outputs[out_key] = out_value
            self.set_multi_output(**outputs)
        self.package_task_item(task_len)
        pipeline_report_tag = self.get_task_item(0, hash_key="report_tag")
        if pipeline_report_tag is not None:
            self.current_task.is_report_task = False
            self.current_task.task_report_tag = pipeline_report_tag
        self.clear_task_item(task_len)

    def fail_pipeline(self, *args):
        task_len = self.get_task_item(0, hash_key="task_len")
        if task_len is None:
            return
        running_count = 0
        for index in range(task_len):
            if self.get_task_item(index + 1, "task_status") == TaskStatus.RUNNING:
                running_count += 1
        if running_count != 0:
            return
        self.package_task_item(task_len)
        pipeline_report_tag = self.get_task_item(0, hash_key="report_tag")
        if pipeline_report_tag is not None:
            self.current_task.is_report_task = False
            self.current_task.task_report_tag = pipeline_report_tag
        self.clear_task_item(task_len)
        self.set_current_task_error(*args)

    def package_task_item(self, task_len=None):
        if task_len is None:
            task_len = self.get_task_item(0, hash_key="task_len")
        if task_len is None:
            return
        pipeline_task = dict(task_list=[])
        pipeline_task.update(self.get_task_item(0))
        for index in range(task_len):
            pipeline_task["task_list"].append(self.get_task_item(index + 1))
        self.current_task.task_name = self.get_task_item(0, hash_key="task_name")
        self.current_task.start_time = pipeline_task["start_time"]
        self.current_task.sub_task_detail = pipeline_task["task_list"]

    def clear_task_item(self, task_len):
        for index in range(task_len + 1):
            self.del_task_item(index)

    def analysis_ref(self, ref_str, current_index, task_len):
        split_d = self.split_ref(ref_str)
        if split_d is None:
            return False, "Input Not Standard Ref Result Format %s" % ref_str
        ref_index = split_d["index"]
        ref_key = split_d["key"]
        if isinstance(current_index, int):
            if ref_index == current_index + 1:
                return False, "Input Can Not Ref Self %s" % ref_str
        if ref_index > task_len:
            return False, "Input Ref Task %s Out Of Index %s" % (ref_index, ref_str)
        if self.get_task_item(ref_index, "task_status") != TaskStatus.SUCCESS and ref_index > 0:
            return True, None
        # 判断 是获得 input 还是 output
        if ref_index == 0:
            ref_output = self.get_task_item(ref_index, hash_key="input_%s" % ref_key)
        else:
            ref_output = self.get_task_item(ref_index, hash_key="output_%s" % ref_key)
        if not ref_output:
            return False, "Input Ref %s Not In Task %s Output. %s" % (ref_key, ref_index, ref_str)
        if is_string(ref_output) is True and ref_output.startswith("&") is True:
            return False, "Ref Output Value Can Not Start With &. %s" % ref_output
        return True, dict(ref_output=ref_output, ref_index=ref_index, ref_key=ref_key)

    def convert_repeat(self, task_item, index):
        input_list_keys = []
        other_keys = ["work_tag"]
        task_output = task_item.get("task_output", dict())
        for item_key in task_item.keys():
            if item_key.startswith("output_"):
                task_output[item_key[7:]] = task_item[item_key]
                continue
            if item_key.startswith("input_") is False:
                continue
            if isinstance(task_item[item_key], list):
                input_list_keys.append(item_key)
            else:
                other_keys.append(item_key)
        task_item["task_output"] = task_output
        if "task_list" in task_item:
            other_keys.append("task_list")
        if task_item["task_type"].endswith("pipeline"):
            other_keys.append("task_output")
        if "repeat_freq" in task_item:
            repeat_freq = task_item["repeat_freq"]
        elif len(input_list_keys) <= 0:
            return task_item
        else:
            repeat_freq = len(task_item[input_list_keys[0]])
        for list_key in input_list_keys:
            if len(task_item[list_key]) != repeat_freq:
                self.set_task_item(index + 1, "task_status", TaskStatus.INVALID)
                self.fail_pipeline("Task ", index + 1, " list input length different")
        pipeline_task = dict(task_list=[], task_output=dict(), task_type="pipeline", work_tag=self.work_tag)
        output_ref_def = dict()
        for output_key in task_output:
            output_value = task_output[output_key]
            if task_item["task_type"].endswith("pipeline"):
                output_ref_def[output_key] = output_key
                pipeline_task["task_output"][output_key] = []
                continue
            if is_string(output_value) is False:
                continue
            if output_value.startswith("&"):
                ov_f = re.findall("^\\d*(\\w+)$", output_value[1:])
                if len(ov_f) != 1:
                    continue
                output_ref_def[output_key] = ov_f[0]
                pipeline_task["task_output"][output_key] = []
            else:
                pass
        for r_index in range(repeat_freq):
            sub_task_item = dict()
            for list_key in input_list_keys:
                sub_task_item[list_key] = task_item[list_key][r_index]
            for other_key in other_keys:
                sub_task_item[other_key] = task_item[other_key]
            for o_key in output_ref_def:
                pipeline_task["task_output"][o_key].append("&%s%s" % (r_index + 1, output_ref_def[o_key]))
            pipeline_task["task_list"].append(sub_task_item)
        return pipeline_task

    def handle_task(self, key, params):
        if self.current_task.is_report_task is False:
            self.task_log("Start Format Pipeline")
            self.format_pipeline(key, params)

        # 获得task_len
        task_len = self.get_task_item(0, "task_len")
        self.task_log("Task Len Is ", task_len)
        for index in range(task_len):
            self.task_log("Start Set Input For Task ", index + 1)
            task_item = self.get_task_item(index + 1)
            if "task_status" in task_item:
                self.task_log("Task ", index + 1, " Not Need Set Input, Status Is ", task_item["task_status"])
                continue
            for item_key in task_item.keys():
                if item_key.startswith("input_") is False:
                    continue
                inp = task_item[item_key]
                if is_string(inp) is True and inp.startswith("&"):
                    self.task_log("Task ", index + 1, " Handle Input ", item_key)
                    ref_r, ref_info = self.analysis_ref(inp[1:], index, task_len)
                    if ref_r is False:
                        self.fail_pipeline("Task ", index + 1, " ", ref_info)
                    if ref_info is None:
                        continue
                    ref_output = ref_info["ref_output"]
                    ref_index = ref_info["ref_index"]
                    ref_key = ref_info["ref_key"]
                    self.task_log("Task ", index + 1, " Input ", item_key, " Ref Task", ref_index, " ", ref_key, " ",
                                  ref_output)
                    self.set_task_item(index + 1, item_key, ref_output)
                elif isinstance(inp, list):
                    for sub_i in range(len(inp)):
                        sub_inp = inp[sub_i]
                        if is_string(sub_inp) is False or sub_inp.startswith("&") is False:
                            continue
                        ref_r, ref_info = self.analysis_ref(sub_inp[1:], index, task_len)
                        if ref_r is False:
                            self.fail_pipeline("Task ", index + 1, " ", ref_info)
                        if ref_info is None:
                            continue
                        ref_output = ref_info["ref_output"]
                        self.set_task_item(index + 1, "%s_%s" % (item_key, sub_i), ref_output)

        running_count = 0
        success_count = 0
        for index in range(task_len):
            task_item = self.get_task_item(index + 1)
            if "task_status" in task_item:
                if task_item["task_status"] == TaskStatus.SUCCESS:
                    success_count += 1
                    continue
                elif task_item["task_status"] == TaskStatus.RUNNING:
                    running_count += 1
                    continue
                else:
                    continue
            is_ready = True
            input_keys = []
            for item_key in task_item.keys():
                if item_key.startswith("input_") is False:
                    continue
                input_keys.append(item_key)
                inp = task_item[item_key]
                if is_string(inp) is True and inp.startswith("&"):
                    is_ready = False
                    break
                elif isinstance(inp, list):
                    for sub_i in range(len(inp)):
                        sub_inp = inp[sub_i]
                        if is_string(sub_inp) and sub_inp.startswith("&"):
                            ref_output = self.get_task_item(index + 1, "%s_%s" % (item_key, sub_i))
                            if ref_output is None:
                                is_ready = False
                                break
                            else:
                                inp[sub_i] = ref_output
                    if is_ready is False:
                        break
            if is_ready is True:
                l = self.set_task_item(index + 1, "task_status", TaskStatus.RUNNING, nx=True)
                if l == 1:
                    if task_item["task_type"].startswith("repeat-"):
                        sub_task_params = self.convert_repeat(task_item, index)
                    else:
                        sub_task_params = task_item
                        for input_key in input_keys:
                            sub_task_params[input_key[6:]] = task_item[input_key]
                    if self.current_task.task_sub_key is None:
                        sub_key = index + 1
                    else:
                        sub_key = "%s_%s" % (self.current_task.task_sub_key, index + 1)
                    self.task_log("Push Task ", index + 1, " Run")
                    self.set_task_item(index + 1, "begin_time", time())
                    self.push_task(key, sub_task_params, sub_key=sub_key, work_tag=sub_task_params["work_tag"],
                                   report_tag=self.work_tag)
                running_count += 1
        if success_count == task_len:
            self.task_log("Task All Success")
            self.completed_pipeline()
        elif running_count == 0:
            task_status = self.get_task_item(0, "task_status")
            if task_status is None:
                self.task_log("Pipeline Has Endless Loop Waiting")
                self.fail_pipeline("Pipeline Has Endless Loop Waiting")
            self.fail_pipeline(self.get_task_item(0, "task_message"))
