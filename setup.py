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
version = "1.0.2"
url = "https://github.com/meisanggou/Tools"
author = __author__
short_description = "Jing Yun Tools Library"
long_description = """Jing Yun Tools Library."""
keywords = "JYTools"
install_requires = ["redis", "six"]

setup(name=name,
      version=version,
      author=author,
      author_email="zhouheng@gene.ac",
      url=url,
      packages=["JYTools", "JYTools/JYWorker", "JYTools/JYFlask"],
      license="MIT",
      description=short_description,
      long_description=long_description,
      keywords=keywords,
      install_requires=install_requires
      )
