#! /usr/bin/env python
# coding: utf-8

#  __author__ = 'meisanggou'

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

import sys

if sys.version_info <= (2, 7):
    sys.stderr.write("ERROR: jingyun tools requires Python Version 2.7 or above.\n")
    sys.stderr.write("Your Python Version is %s.%s.%s.\n" % sys.version_info[:3])
    sys.exit(1)

name = "JYTools"
version = "0.1.20"
url = "https://github.com/meisanggou/Tools"
license = "MIT"
author = "meisanggou"
short_description = "Jing Yun Tools Library"
long_description = """Jing Yun Tools Library."""
keywords = "JYTools"
install_requires = ["MySQL-python", "redis >= 2.10.5", "requests"]

setup(name=name,
      version=version,
      author=author,
      author_email="zhouheng@gene.ac",
      url=url,
      packages=["JYTools", "JYTools/JYWorker"],
      license=license,
      description=short_description,
      long_description=long_description,
      keywords=keywords,
      install_requires=install_requires
      )
