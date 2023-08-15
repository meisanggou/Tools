#! /usr/bin/env python
# coding: utf-8

import sys
import logging
from JYTools.MyEmail import EmailManager
from JYTools import StringTool


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
    return input(prompt)
