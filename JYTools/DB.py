# !/usr/bin/env python
# encoding: utf-8

from mysqldb_rich import DB

__author__ = 'meisanggou'


if __name__ == "__main__":
    db = DB()
    items = db.execute_select("zh_test", prefix_value=dict(a=r"abc_", b="%"), cols=["a"], print_sql=True)
    for item in items:
        print(item)