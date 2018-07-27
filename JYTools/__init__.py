#! /usr/bin/env python
# coding: utf-8

import sys
import logging
from MyEmail import EmailManager
import StringTool


__author__ = 'meisanggou'


TIME_FORMAT = '%Y-%m-%d %H:%M:%S'

DEFAULT_ENCODING = "utf-8"
SECOND_ENCODING = "gbk"

logger_name = "JYWorker"
logger = logging.getLogger(logger_name)
if len(logger.handlers) <= 0:
    sh = logging.StreamHandler()
    fmt = logging.Formatter(fmt="%(levelname)s: %(message)s")
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    logger.setLevel(logging.INFO)


def jy_input(prompt, prompt_prefix=None):
    if prompt_prefix is not None and prompt is not None:
        prompt = StringTool.join([prompt_prefix, prompt], "")
    if not prompt.endswith("\n"):
        prompt += "\n"
    if sys.version_info[0] == 2:
        return raw_input(prompt)
    else:
        return input(prompt)


def is_num(v):
    return isinstance(v, (int, long))


def is_number(v):
    return is_num(v)
