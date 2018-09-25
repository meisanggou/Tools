#! /usr/bin/env python
# coding: utf-8

import re
import json
import logging
from time import time
from JYTools import is_num, logger
from JYTools.StringTool import is_string, join_decode
from JYTools.util.file import FileWriter
from JYTools.JYWorker.util import ValueVerify, ReportScene
from ._Task import TaskStatus, TaskType
from ._redis import RedisWorker

__author__ = 'meisanggou'


class DAGTools(object):
    RIGHT_TASK_TYPE = ("app", "pipeline", "repeat-app", "repeat-pipeline")

    @classmethod
    def _verify_pipeline_attribute(cls, p_params):
        if "task_list" not in p_params:
            error_msg = join_decode("pipeline应该包含task_list属性")
            logger.error(error_msg)
            return False, dict(code=2, data="task_list", message=error_msg)
        tl = p_params["task_list"]
        if isinstance(tl, list) is False:
            error_msg = join_decode(["task_list属性值的类型不正确。应该是list，传入的不是list类型，现在是", type(tl)])
            logger.error(error_msg)
            return False, dict(code=3, data="task_list", message=error_msg)
        if len(tl) <= 0:
            error_msg = join_decode("pipeline应该至少包含一个任务")
            logger.error(error_msg)
            return False, dict(code=4, data=len(tl), message=error_msg)
        if "task_output" in p_params:
            task_output = p_params["task_output"]
            if isinstance(task_output, dict) is False:
                error_msg = join_decode(["task_output属性值的类型不正确。应该是dict，传入的不是dict类型，现在是",
                                         type(task_output)])
                return False, dict(code=5, data="task_output", message=error_msg)
            for key in task_output.keys():
                p_params["output_%s" % key] = task_output[key]
        return True, dict(code=0, data=None, message="success")

    @classmethod
    def _verify_pipeline_input_output(cls, p_params):
        tl = p_params["task_list"]
        output_keys = filter(lambda x: x.startswith("output_"), p_params.keys())
        if len(output_keys) <= 0:
            # warn 7 pipeline未设置一个输出
            warn_msg = "pipeline未设置一个输出，一般不会这样设置"
            logger.warning(warn_msg)
        for key in output_keys:
            if is_string(p_params[key]) is False:
                continue
            if p_params[key].startswith("&") is False:
                continue
            ref_d = DAGWorker.split_ref(p_params[key])
            ref_task = ref_d["ref_task"]
            if ref_task <= 0 or ref_task > len(tl):
                error_msg = join_decode(["pipeline的输出[", key, "]在视图引用子任务[", ref_task,
                                         "]的输出，但是这个子任务不存在"])
                logger.error(error_msg)
                return False, dict(code=7, data="%s|%s" % (key, ref_task), message=error_msg)
            ref_index = ref_task - 1
            ref_key = ref_d["key"]
            if TaskStatus.is_success(tl[ref_index]["task_status"]) or tl[ref_index]["task_type"] != "app":
                output_key = "output_" + ref_d["key"]
                if output_key not in tl[ref_index]:
                    error_msg = join_decode(["pipeline的输出[", key, "]在试图引用子任务[", ref_task, "]的输出[", ref_key,
                                             "]，但是在这个子任务中并没有发现这个输出"])
                    logger.error(error_msg)
                    return False, dict(code=8, data="%s|%s|%s" % (key, ref_task, ref_key), message=error_msg)
        input_keys = filter(lambda x: x.startswith("input_"), p_params.keys())
        for key in input_keys:
            if is_string(p_params[key]) is False:
                continue
            if p_params[key].startswith("&") is False:
                continue
            # warn 6 pipeline输入为字符串类型，而且以&开头
            warn_msg = join_decode(["父任务的输入[", key, "]是个以&开头的字符串[", p_params[key],
                                    "]，如果子任务引用到将会出错"])
            logger.warning(warn_msg)
        return True, dict(code=0, data=None, message="success")

    @classmethod
    def _verify_pipeline_item(cls, index, item):
        task_no = index + 1
        if "task_type" not in item:
            item["task_type"] = "app"
        if "task_status" not in item:
            item["task_status"] = TaskStatus.NONE
        if item["task_type"] not in cls.RIGHT_TASK_TYPE:
            error_msg = "invalid task_type, now index is %s, task_type is %s" % (index, item["task_type"])
            logger.error(error_msg)
            return False, dict(code=11, data=item["task_type"], message=error_msg)
        if "task_output" in item:
            task_output = item["task_output"]
            if isinstance(task_output, dict) is False:
                error_msg = "task_output need dict type, now index is %s, task_output type is %s" \
                            % (index, type(task_output))
                logger.error(error_msg)
                return False, dict(code=12, data=type(task_output), message=error_msg)
            for key in task_output.keys():
                item["output_%s" % key] = task_output[key]

        # 检查work_tag -------------------------------------------------------------------------------------------------
        if item["task_type"] in ["app", "repeat-app"]:
            if "work_tag" not in item:
                error_msg = join_decode(["app类型的子任务需要包含work_tag属性，任务[", task_no, "]不符合要求"])
                logger.error(error_msg)
                return False, dict(code=13, data="work_tag", message=error_msg)
            work_tag = item["work_tag"]
            if is_string(work_tag) is False:
                error_msg = join_decode(["work_tag属性对应值，必须是字符串类型的，子任务[", task_no, "]的work_tag是[",
                                         type(work_tag), "]类型的"])
                logger.error(error_msg)
                return False, dict(code=14, data=type(work_tag), message=error_msg)
            if ValueVerify.v_work_tag(work_tag) is False:
                error_msg = join_decode(["work_tag属性对应值，仅允许包含数字字母下划线短横线，子任务[", task_no,
                                         "]的work_tag是[", work_tag, "]"])
                logger.error(error_msg)
                return False, dict(code=14, data=type(work_tag), message=error_msg)
        item_keys = item.keys()
        # 检查输出 ------------------------------------------------------------------------------------------------------
        output_keys = filter(lambda x: x.startswith("output_"), item_keys)
        if TaskStatus.is_success(item["task_status"]):
            if len(output_keys) <= 0:
                # warn 2 某个子任务的状态为success时，没有设置一个输出
                warn_msg = "子任务[%s]已经是完成状态，但是没有发现输出参数，一般应该有输出参数的" % task_no
                logger.warning(warn_msg)
            else:
                for key in output_keys:
                    # warn 3 某个子任务的状态为success时，输出值为&开头的字符串
                    if is_string(item[key]) and item[key].startswith("&"):
                        warn_msg = join_decode(["子任务[", task_no, "]的一个输出[", key, "]输出值为[", item[key],
                                                "]，不应该以&开头，如果其他任务引用了该输出将会报错"])
                        logger.warning(warn_msg)
        elif item["task_type"] == "app":
            # warn 1 某个app类型的子任务的状态不为success时，但是设置了task_output
            if len(output_keys) > 0:
                warn_msg = "子任务[%s]的状态还未成功，但发现了输出设置，我们一般不这么干" % task_no
                logger.warning(warn_msg)
        else:
            # warn 4 某个非app类型的子任务没有设置一个输出
            if len(output_keys) <= 0:
                warn_msg = join_decode(["子任务[", task_no, "],任务类型为[", item["task_type"],
                                        "]，没有发现一个输出参数，一般应该有输出参数"])
                logger.warning(warn_msg)
        # 检查repeat_freq
        if item["task_type"].startswith("repeat-") is True:
            if "repeat_freq" in item:
                repeat_freq = item["repeat_freq"]
                if is_num(repeat_freq) is False or repeat_freq <= 0:
                    error_msg = join_decode(["子任务[", task_no, "]设置了repeat_freq,设置为[", repeat_freq,
                                             "]，不是数字或者不大于0"])
                    return False, dict(code=15, data=repeat_freq, message=error_msg)

        # 检查是否有多余的key---------------------------------------------------------------------------------------------
        avail_keys = ["task_type", "task_output", "task_status"]
        if item["task_type"].endswith("pipeline") is True:
            avail_keys.append("task_list")
        else:
            avail_keys.append("work_tag")
        if item["task_type"].startswith("repeat-") is True:
            avail_keys.append("repeat_freq")
        surplus_keys = filter(lambda x: x not in avail_keys, item_keys)
        surplus_keys = filter(lambda x: x.startswith("input_") is False, surplus_keys)
        surplus_keys = filter(lambda x: x.startswith("output_") is False, surplus_keys)
        for key in surplus_keys:
            warn_msg = join_decode(["子任务[", task_no, "]包含一个无用的属性[", key, "]"])
            logger.warning(warn_msg)
        return True, dict(code=0, data=None, message="success")

    @staticmethod
    def _verify_ref(p_params):
        tl = p_params["task_list"]
        task_len = len(tl)
        rs_l = [dict(quotes=list(), next=list(), index=i) for i in range(task_len)]

        completed_queue = [0]

        for index in range(task_len):
            task_item = tl[index]
            task_no = index + 1
            if TaskStatus.is_success(task_item["task_status"]) is True:
                completed_queue.append(task_no)
                continue
            for k, v in task_item.items():
                if k.startswith("input_") is False:
                    continue
                if is_string(v) is False:
                    continue
                if v.startswith("&") is False:
                    continue
                ref_d = DAGWorker.split_ref(v)
                if ref_d is None:
                    error_msg = join_decode(["子任务[", task_no, "]的输入[", k,
                                             "]为字符串且以&开头，但不是一个合法引用格式"])
                    logger.error(error_msg)
                    return False, dict(code=21, data=k, message=error_msg)
                ref_key = ref_d["key"]
                ref_task = ref_d["ref_task"]
                if ref_d["index"] < 0 or ref_d["index"] > task_len:
                    error_msg = join_decode(["子任务[", task_no, "]的输入[", k, "]在试图引用子任务[", ref_d["index"],
                                             "]的输出，但是这个子任务不存在"])
                    logger.error(error_msg)
                    return False, dict(code=16, data="", message=error_msg)
                if ref_d["index"] not in rs_l[index]["quotes"]:
                    rs_l[index]["quotes"].append(ref_d["index"])
                    if ref_d["index"] > 0:
                        rs_l[ref_d["index"] - 1]["next"].append(index)
                if ref_d["required"] is False:
                    continue
                if ref_d["ref_task"] == 0:
                    input_key = "input_" + ref_d["key"]
                    if input_key not in p_params:
                        error_msg = join_decode(["子任务[", task_no, "]的输入[", k, "]在试图引用父任务的输入[",
                                                 ref_d["key"], "]，但是在父任务中并没有发现这个输入"])
                        logger.error(error_msg)
                        return False, dict(code=17, data="%s|%s|%s" % (task_no, k, ref_d["key"]), message=error_msg)
                    input_v = p_params[input_key]
                    if is_string(input_v) and input_v.startswith("&"):
                        error_msg = join_decode(["子任务[", task_no, "]的输入[", k, "]在试图引用父任务的输入[", ref_key,
                                                 "]，但是在父任务的输入为字符串类型，而且以&开头，这是不被允许的"])
                        logger.error(error_msg)
                        return False, dict(code=18, data="%s|%s|%s" % (task_no, k, ref_key), message=error_msg)
                    continue
                if ref_d["ref_task"] == task_no:
                    error_msg = join_decode(["子任务[", task_no, "]的输入[", k, "]在试图引用自己的输出[", ref_d["key"],
                                             "]，这是很搞笑的"])
                    logger.error(error_msg)
                    return False, dict(code=19, data="%s|%s|%s" % (task_no, k, ref_d["key"]), message=error_msg)
                ref_index = ref_d["ref_task"] - 1
                if TaskStatus.is_success(tl[ref_index]["task_status"]) or tl[ref_index]["task_type"] != "app":
                    output_key = "output_" + ref_d["key"]
                    if output_key not in tl[ref_index]:
                        error_msg = join_decode(["子任务[", task_no, "]的输入[", k, "]在试图引用子任务[", ref_task,
                                                 "]的输出[", ref_key, "]，但是在这个子任务中并没有发现这个输出"])
                        logger.error(error_msg)
                        return False, dict(code=20, data="%s|%s|%s|%s" % (task_no, k, ref_task, ref_key),
                                           message=error_msg)

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
                return True, dict()
            if completed_num == 0:
                error_msg = "各个子任务之间引用存在回路"
                logger.error(error_msg)
                return False, dict(code=6, data=None, message=error_msg)
        return True, dict(code=0, data=None, message="success")

    @classmethod
    def ip_verify_pipeline(cls, p_params):
        """
        1 校验pipeline中是否有引用过界
        2 校验pipeline结构中是否包含回路
        3 引用为pipeline输入时，校验输入是否存在
        4 引用为某个pipeline类型的子任务时，校验输出是否存在
        5 引用为某个app类型的子任务的输出，且该任务状态为success时，校验输出是否存在
        6 Pipeline输出引用子任务不存在

        以下情况给出警告
        1 某个app类型的子任务的状态不为success时，但是设置了task_output
        2 某个子任务的状态为success时，没有设置一个输出
        3 某个子任务的状态为success时，输出值为&开头的字符串
        4 某个非app类型的子任务没有设置一个输出
        5 子任务包含无用的属性
        6 pipeline输入为字符串类型，而且以&开头
        7 pipeline未设置一个输出
        8 pipeline设置的输出无法在子任务中找到
        :param p_params:
        :return:
        error_code:
        1 pipeline结构应该是个字典类型
        2 pipeline中必须存在一个属性task_list，但是没有存在
        3 pipeline的属性值task_list的类型不正确。应该是list，传入的不是list类型
        4 pipeline至少包含一个任务
        5 pipeline的task_output属性值的类型不正确。应该是dict，传入的不是dict类型
        6 pipeline结构中包含回路
        7 pipeline设置的输出无法找到对应的子任务
        8 pipeline设置的输出无法在对应子任务中找到引用的输出

        11 pipeline子任务的任务类型task_type不合法
        12 pipeline子任务的task_output应该是字典dict类型的
        13 pipeline中app和repeat-app类型子任务必须设置work_tag
        14 pipeline中app和repeat-app类型子任务属性work_tag类型必须是字符串类型的而且仅允许包含数字字母下划线短横线
        15 pipeline中repeat-app和repeat-pipeline类型子任务属性repeat_freq必须是数字类型，而且必须大于0
        16 pipeline子任务的输入引用了一个不存在的任务的输出
        17 pipeline子任务的输入引用了一个父任务不存在的输入
        18 pipeline子任务的输入引用了一个父任务的输入，但是这个父任务的输入为字符串类型，而且以&开头
        19 pipeline子任务的输入引用了自己的输出
        20 pipeline子任务的输入引用了另一个子任务的输出，但是该输出不存在
        21 pipeline子任务的输入为字符串且以&开头，但不是一个合法引用格式

        """
        logger.warning(u"你正在调用一个处于试用阶段的方法，测试结果仅供参考，请勿用于生产环境")
        if isinstance(p_params, dict) is False:
            error_msg = join_decode(["pipeline结构应该是个字典类型，现在是", type(p_params)])
            return False, dict(code=1, data=None, message=error_msg)
        # 格式化pipeline的属性
        r, data = cls._verify_pipeline_attribute(p_params)
        if r is False:
            return r, data

        # format子任务 检测pipeline子任务的格式
        tl = p_params["task_list"]
        for index in range(len(tl)):
            item = tl[index]
            r, data = cls._verify_pipeline_item(index, item)
            if r is False:
                return r, data

        r, data = cls._verify_pipeline_input_output(p_params)
        if r is False:
            return r, data

        r, data = cls._verify_ref(p_params)
        if r is False:
            return r, data
        return True, dict(code=0, data=p_params, message="success")


class DAGWorker(RedisWorker):
    expect_params_type = dict
    ref_compile = re.compile(r"^(\d{1,10})((&\d+|&*[a-z])\w{0,60})(\**)$", re.I)

    def __init__(self, conf_path=None, heartbeat_value=None, is_brother=False, work_tag=None, log_dir=None,
                 redis_host=None, redis_password=None, redis_port=None, redis_db=None, section_name="Redis", **kwargs):
        self.agent_tag = kwargs.pop("agent_tag", None)
        RedisWorker.__init__(self, conf_path, heartbeat_value, is_brother, work_tag, log_dir, redis_host,
                             redis_password, redis_port, redis_db, section_name, **kwargs)
        self.after_handle_funcs.append(self.after_handle)

    def push_task(self, key, params, work_tag=None, sub_key=None, report_tag=None, is_report=False,
                  report_scene=None):
        if self.agent_tag is not None:
            if work_tag is not None and work_tag not in (self.work_tag, self.upload_log_tag) and is_report is False:
                params = dict(work_tag=work_tag, params=params)
                work_tag = self.agent_tag
        if report_scene is None:
            report_scene = ReportScene.Begin | ReportScene.End
        self._push_task(key, params, work_tag, sub_key, report_tag, is_report=is_report, report_scene=report_scene)

    @staticmethod
    def split_ref(ref_str):
        """

        :param ref_str: index+字母开头的key index+&+数字开头的key index+&+字母开头的key
                        最后可以加入*结尾，也可以不加入，暂时定义为required
        :return:
        """
        if ref_str[0] == "&":
            ref_str = ref_str[1:]
        match_r = DAGWorker.ref_compile.match(ref_str)
        if match_r is None:
            return None
        ref_index = int(match_r.groups()[0])
        ref_key = match_r.groups()[1]
        required = match_r.groups()[3]
        if ref_key[0] == "&":
            ref_key = ref_key[1:]
        if required == "*":
            required = False
        else:
            required = True
        return dict(index=ref_index, key=ref_key, required=required, ref_task=ref_index)

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
                if v.startswith("&") is False:
                    continue
                ref_d = DAGWorker.split_ref(v)
                if ref_d is None:
                    raise ValueError("")  # invalid ref str
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

    def get_sub_task_sub_key(self, index=None, task_index=None):
        if index is not None:
            task_index = index + 1
        if self.current_task.task_sub_key is None:
            sub_key = "%s" % task_index
        else:
            sub_key = "%s_%s" % (self.current_task.task_sub_key, task_index)
        return sub_key

    def handle_report_task(self):
        r_task = self.current_task.task_params
        sp_keys = self.current_task.task_sub_key.rsplit("_", 1)
        if len(sp_keys) == 2:
            self.current_task.task_sub_key = sp_keys[0]
        else:
            self.current_task.task_sub_key = None
        reporter_sub_key = int(sp_keys[-1])  # 子任务在父任务中的位置 位置从1开始
        self.task_log("Task ", r_task.work_tag, reporter_sub_key, " Report")
        task_status = r_task.task_status
        # 防止重新汇报 只允许Running状态下可以多次汇报
        old_status = self.get_task_item(reporter_sub_key, "task_status")
        if old_status == task_status and not TaskStatus.is_running(task_status):
            self.task_log("Task", reporter_sub_key, "Old Status is", old_status, ". This Report Is Same. IGNORE")
            return
        task_message = r_task.task_message
        self.task_log("Task ", reporter_sub_key, " Status Is ", task_status)
        self.set_task_item(reporter_sub_key, "task_status", task_status)
        self.set_task_item(reporter_sub_key, "task_message", task_message)
        if r_task.sub_task_detail is not None:
            self.set_task_item(reporter_sub_key, "task_list", r_task.sub_task_detail)
        if TaskStatus.is_running(task_status) is True:
            return
        if TaskStatus.is_success(task_status) is False:
            self.set_task_item(0, "task_message", task_message)
            self.set_task_item(0, "task_fail_index", reporter_sub_key)
            self.task_log("Sub Task ", r_task.task_sub_key, " ", r_task.work_tag, " Failed", level="ERROR")
            self.fail_pipeline("Sub Task ", r_task.task_sub_key, " ", r_task.work_tag, " Failed,", task_message)
        if isinstance(r_task.task_output, dict):
            for output_key in r_task.task_output.keys():
                self.set_task_item(reporter_sub_key, "output_%s" % output_key, r_task.task_output[output_key])
        self.set_task_item(reporter_sub_key, "start_time", r_task.start_time)
        self.set_task_item(reporter_sub_key, "end_time", r_task.end_time)
        self.set_task_item(reporter_sub_key, "finished_time", time())
        # 获取当前pipeline状态
        pipeline_status = self.get_task_item(0, hash_key="task_status")
        if TaskStatus.is_fail(pipeline_status):
            self.task_log("Current Task Status Is ", pipeline_status)
            return self.try_finish_pipeline()
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
                if TaskStatus.is_success(task_item["task_status"]) is False:
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
            self.set_task_item(0, "report_scene", self.current_task.task_report_scene)
            self.current_task.task_report_tag = None  # 真正执行完后才进行report
        self.set_task_item(0, "task_output", task_output)
        self.set_task_item(0, "start_time", time())
        self.set_task_item(0, "task_status", TaskStatus.RUNNING)
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
                        self.task_log(ref_info, level="WARNING")
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
                            self.task_log(ref_info, level="WARNING")
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
            self.current_task.task_type = TaskType.Normal
        self.clear_task_item(task_len)

    def try_remove_ready_or_running_task(self):
        self.task_log("Try to remove or stop ready or running task")
        task_len = self.get_task_item(0, hash_key="task_len")
        if task_len is None:
            self.set_current_task_error("Not Found Pipeline Task Len")
            return False
        for index in range(task_len):
            task_index = index + 1
            if TaskStatus.is_ready(self.get_task_item(task_index, "task_status")) is False:
                continue
            work_tag = self.get_task_item(task_index, "work_tag")
            self.task_log("Try to remove task %s %s" % (task_index, work_tag))
            sub_key = self.get_sub_task_sub_key(task_index=task_index)
            count = self.stat_man.remove_queue_task(work_tag, self.current_task.task_key, self.work_tag, sub_key)
            if count > 0:
                self.task_log("Remove task %s %s SUCCESS" % (task_index, work_tag))
                self.set_task_item(task_index, hash_key="task_status", hash_value=TaskStatus.NONE)

    def try_finish_pipeline(self):
        """
        若无正在运行的任务，清理pipeline的调度信息，打包运行结果，汇报结果。返回True
        若有正在运行的任务。返回False
        :return:
        """
        self.task_log("Try Finish Pipeline")
        task_len = self.get_task_item(0, hash_key="task_len")
        if task_len is None:
            self.set_current_task_error("Not Found Pipeline Task Len")
            return False
        running_count = 0
        ready_count = 0
        for index in range(task_len):
            task_status = self.get_task_item(index + 1, "task_status")
            if TaskStatus.is_running(task_status) is True:
                running_count += 1
            if TaskStatus.is_ready(task_status) is True:
                ready_count += 1
        if running_count + ready_count != 0:
            return False
        self.package_task_item(task_len)
        pipeline_report_tag = self.get_task_item(0, hash_key="report_tag")
        if pipeline_report_tag is not None:
            self.current_task.is_report_task = False
            self.current_task.task_report_tag = pipeline_report_tag
            self.current_task.task_type = TaskType.Normal
        self.clear_task_item(task_len)
        self.current_task.task_status = TaskStatus.FAIL
        # 自动保存fail掉的任务详情
        with FileWriter(self.current_task.log_path + ".r") as w:
            w.write(json.dumps(self.current_task.to_dict(), indent=2))
        return True

    def fail_pipeline(self, *args):
        """
        若无正在运行的任务，清理pipeline的调度信息，打包运行结果，汇报结果
        若有正在运行的任务，终止此次处理，通报错误
        :param args:
        :return:
        """
        # set pipeline status is fail
        self.set_task_item(0, "task_status", TaskStatus.FAIL)

        # set task errors
        task_errors = self.get_task_item(0, hash_key="task_errors")
        if isinstance(task_errors, list) is False:
            task_errors = []
        task_errors.append(join_decode(args))
        self.set_task_item(0, hash_key="task_errors", hash_value=task_errors)
        # 若error_continue为False尽最大可能删除正在运行的任务
        error_continue = self.get_task_item(0, hash_key="error_continue")
        if error_continue is not True:  # 如果任务设置error_continue不为True Pipeline有失败时，尝试删除已放入队列的任务
            self.try_remove_ready_or_running_task()
        self.try_finish_pipeline()
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

        task_errors = self.get_task_item(0, hash_key="task_errors")
        if isinstance(task_errors, list) is True:
            self.current_task.add_error_msg(*task_errors)
        self.current_task.task_name = self.get_task_item(0, hash_key="task_name")
        self.current_task.start_time = pipeline_task["start_time"]
        self.current_task.sub_task_detail = pipeline_task["task_list"]

    def clear_task_item(self, task_len):
        self.task_log("Start Clear Pipeline Task Item, Task Len Is ", task_len)
        for index in range(task_len + 1):
            self.del_task_item(index)

    def analysis_ref(self, ref_str, current_index, task_len, allow_non_required=False):
        """

        :param ref_str:
        :param current_index:
        :param task_len:
        :param allow_non_required:
        :return:
        若allow_non_required为False，返回的第一个参数为True，第二个参数肯定包含ref_output
        若allow_non_required为True，返回的第一个参数为True，第二个参数可以不包含ref_output
        """
        split_d = self.split_ref(ref_str)
        if split_d is None:
            return False, "Input Not Standard Ref Result Format %s" % ref_str
        ref_index = split_d["index"]
        ref_key = split_d["key"]
        required = split_d["required"]
        if required is False and allow_non_required is False:
            return False, "Input Not Standard Ref Result Format %s, Not Allow * In The End." % ref_str

        if isinstance(current_index, int):
            if ref_index == current_index + 1:
                return False, "Input Can Not Ref Self %s" % ref_str
        if ref_index > task_len:
            return False, "Input Ref Task %s Out Of Index %s" % (ref_index, ref_str)
        if TaskStatus.is_success(self.get_task_item(ref_index, "task_status")) is False and ref_index > 0:
            return True, None
        # 判断 是获得 input 还是 output
        ana_data = dict(ref_index=ref_index, ref_key=ref_key)
        if ref_index == 0:
            if self.has_task_item(ref_index, hash_key="input_%s" % ref_key) is False:
                if required is False:
                    return True, ana_data
                return False, "Input Ref %s Not In Task %s Input. %s" % (ref_key, ref_index, ref_str)
            ref_output = self.get_task_item(ref_index, hash_key="input_%s" % ref_key)
        else:
            if self.has_task_item(ref_index, hash_key="output_%s" % ref_key) is False:
                if required is False:
                    return True, ana_data
                work_tag = self.get_task_item(ref_index, hash_key="work_tag")
                return False, "Input Ref %s Not In Task %s %s Output. [%s]" % (ref_key, ref_index, work_tag, ref_str)
            ref_output = self.get_task_item(ref_index, hash_key="output_%s" % ref_key)
        if is_string(ref_output) is True and ref_output.startswith("&") is True:
            return False, "Ref Output Value Can Not Start With &. [%s]" % ref_output
        if isinstance(ref_output, list) is True:
            for item in ref_output:
                if is_string(item) is True and item.startswith("&") is True:
                    work_tag = self.get_task_item(ref_index, hash_key="work_tag")
                    msg = "Ref task %s %s output %s value is list, each item can not start with &, but exist item " \
                          "value is %s" % (ref_index, work_tag, ref_key, item)
                    return False, msg
        ana_data["ref_output"] = ref_output
        return True, ana_data

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
            repeat_freq = 1
        else:
            repeat_freq = max(map(lambda x: len(task_item[x]), input_list_keys))
        for list_key in input_list_keys:
            k_l = len(task_item[list_key])
            if repeat_freq % k_l != 0:
                self.set_task_item(index + 1, "task_status", TaskStatus.INVALID)
                self.set_task_item(index + 1, "task_message", "list input length different")
                self.fail_pipeline("Task ", index + 1, " list input length different")
            task_item[list_key] *= repeat_freq / k_l
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
                ov_f = re.findall("^\\d*&*(\\w+)$", output_value[1:])
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
            # 检测是否有同样的KEY在调度
            task_len = self.get_task_item(0, "task_len")
            self.task_log("Task Len Is ", task_len)
            if task_len is not None:
                self.set_current_task_invalid("Have the same key in scheduling, replace key is recommended")
            self.task_log("Start Format Pipeline")
            sh = logging.StreamHandler(self)
            fmt = logging.Formatter(fmt="%(levelname)s: %(message)s")
            sh.setFormatter(fmt)
            logger_level = logger.level
            logger.addHandler(sh)
            logger.setLevel(logging.WARNING)
            # 检测pipeline结构，仅做参考暂不真实应用
            DAGTools.ip_verify_pipeline(params)

            logger.removeHandler(sh)
            logger.setLevel(logger_level)
            self.format_pipeline(key, params)

        # 获得task_len
        task_len = self.get_task_item(0, "task_len")
        if task_len is None:
            self.set_current_task_error("Task Len Is None.")
        self.task_log("Task Len Is ", task_len)
        for index in range(task_len):
            self.task_log("Start Set Input For Task ", index + 1)
            task_item = self.get_task_item(index + 1)
            task_status = task_item.get("task_status", None)
            if TaskStatus.is_none(task_status) is False:
                self.task_log("Task ", index + 1, " Not Need Set Input, Status Is ", task_item["task_status"])
                continue
            for item_key in task_item.keys():
                if item_key.startswith("input_") is False:
                    continue
                inp = task_item[item_key]
                if is_string(inp) is True and inp.startswith("&"):
                    self.task_log("Task ", index + 1, " Handle Input ", item_key)
                    ref_r, ref_info = self.analysis_ref(inp[1:], index, task_len, allow_non_required=True)
                    if ref_r is False:
                        self.set_task_item(index + 1, "task_status", TaskStatus.FAIL)
                        self.set_task_item(index + 1, "task_message", ref_info)
                        self.fail_pipeline("Task ", index + 1, " ", ref_info)
                    if ref_info is None:
                        continue
                    # 若ref_output在返回中说明，拿到了引用值。
                    # 若不在返回中说明，未拿到引用值并且该引用为可选引用.将该输入从该任务中删除
                    ref_index = ref_info["ref_index"]
                    ref_key = ref_info["ref_key"]
                    if "ref_output" in ref_info:
                        ref_output = ref_info["ref_output"]
                        self.task_log("Task ", index + 1, " Input ", item_key, " Ref Task", ref_index, " ", ref_key,
                                      " ", ref_output)
                        self.set_task_item(index + 1, item_key, ref_output)
                    else:
                        self.task_log("Task ", index + 1, " Input ", item_key, " Ref Task", ref_index, " ", ref_key,
                                      " , But not found and the input is not required, so delete this input", )
                        self.del_task_item(index + 1, hash_key=item_key)
                elif isinstance(inp, list):
                    for sub_i in range(len(inp)):
                        sub_inp = inp[sub_i]
                        if is_string(sub_inp) is False or sub_inp.startswith("&") is False:
                            continue
                        ref_r, ref_info = self.analysis_ref(sub_inp[1:], index, task_len)
                        if ref_r is False:
                            self.set_task_item(index + 1, "task_status", TaskStatus.FAIL)
                            self.set_task_item(index + 1, "task_message", ref_info)
                            self.fail_pipeline("Task ", index + 1, " ", ref_info)
                        if ref_info is None:
                            continue
                        ref_output = ref_info["ref_output"]
                        self.set_task_item(index + 1, "%s_%s" % (item_key, sub_i), ref_output)

        ready_count = 0
        success_count = 0
        for index in range(task_len):
            task_item = self.get_task_item(index + 1)
            if "task_status" in task_item:
                if TaskStatus.is_success(task_item["task_status"]):
                    success_count += 1
                    continue
                elif TaskStatus.is_running(task_item["task_status"]):
                    ready_count += 1
                    continue
                elif TaskStatus.is_ready(task_item["task_status"]):
                    ready_count += 1
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
                l = self.set_task_item(index + 1, "task_status", TaskStatus.READY, nx=True)
                if l == 1:
                    if task_item["task_type"].startswith("repeat-"):
                        sub_task_params = self.convert_repeat(task_item, index)
                    else:
                        sub_task_params = task_item
                        for input_key in input_keys:
                            sub_task_params[input_key[6:]] = sub_task_params[input_key]
                    if self.current_task.task_sub_key is None:
                        sub_key = index + 1
                    else:
                        sub_key = "%s_%s" % (self.current_task.task_sub_key, index + 1)
                    self.task_log("Task ", index + 1, " ", task_item["work_tag"], " Is Ready, Push to Queue")
                    self.set_task_item(index + 1, "begin_time", time())
                    self.push_task(key, sub_task_params, sub_key=sub_key, work_tag=sub_task_params["work_tag"],
                                   report_tag=self.work_tag)
                ready_count += 1
        if success_count == task_len:
            self.task_log("Task All Success")
            self.completed_pipeline()
        elif ready_count == 0:
            task_status = self.get_task_item(0, "task_status")
            if task_status is None:
                self.task_log("Pipeline Has Endless Loop Waiting")
                self.fail_pipeline("Pipeline Has Endless Loop Waiting")
            return self.try_finish_pipeline()

    def after_handle(self):
        self.task_debug_log("enter after handle")
        pipeline_status = self.get_task_item(0, hash_key="task_status")
        if TaskStatus.is_running(pipeline_status) is False:
            return
        self.task_debug_log("Pipeline is Running.")
        pipeline_report_tag = self.get_task_item(0, hash_key="report_tag")
        if pipeline_report_tag is not None:
            report_scene = self.get_task_item(0, hash_key="report_scene")
            if ReportScene.include_real_time(report_scene):
                self.package_task_item()
                self.current_task.task_status = TaskStatus.RUNNING
                self.current_task.is_report_task = False
                self.current_task.task_report_tag = pipeline_report_tag
