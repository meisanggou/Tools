#! /usr/bin/env python
# coding: utf-8

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import sys

__author__ = 'meisanggou'

if sys.version_info <= (2, 7):
    sys.stderr.write("ERROR: jingyun tools requires Python Version 2.7 or above.\n")
    sys.stderr.write("Your Python Version is %s.%s.%s.\n" % sys.version_info[:3])
    sys.exit(1)

name = "JYTools"
version = "1.7.7"
url = "https://github.com/meisanggou/Tools"
author = __author__
short_description = "Jing Yun Tools Library"
long_description = """Jing Yun Tools Library."""
keywords = "JYTools"
install_requires = ["redis", "six"]


entry_points = {'console_scripts': [
    'dag-worker=JYTools.JYWorker.DAG:main',
    'jyworker.list-queue=JYTools.JYWorker.cli:list_queue',
    'jyworker.list-worry-queue=JYTools.JYWorker.cli:list_worry_queue',
    'jyworker.list-heartbeat=JYTools.JYWorker.cli:list_heartbeat',
    'jyworker.del-heartbeat=JYTools.JYWorker.cli:delete_heartbeat',
    'jyworker.stop-worker=JYTools.JYWorker.cli:delete_heartbeat',
    'jyworker.wash-worker=JYTools.JYWorker.cli:wash_worker',
    'jyworker.list-worker=JYTools.JYWorker.cli:list_worker',
    'jyworker.look-item=JYTools.JYWorker.cli:look_task_item',
    'jyworker.dag-verify=JYTools.JYWorker.cli:verify_pipeline',
    'jyworker.push-task=JYTools.JYWorker.cli:push_task',
    'jyworker.report-task=JYTools.JYWorker.cli:report_task'
]}

setup(name=name,
      version=version,
      author=author,
      author_email="zhouheng@gene.ac",
      url=url,
      packages=["JYTools", "JYTools/JYWorker", "JYTools/JYFlask", "JYTools/util"],
      license="MIT",
      description=short_description,
      long_description=long_description,
      keywords=keywords,
      install_requires=install_requires,
      entry_points=entry_points
      )
