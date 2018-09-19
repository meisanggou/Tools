#! /usr/bin/env python
# coding: utf-8

from JYTools import StringTool

__author__ = '鹛桑够'


class FileWriter(object):

    def __init__(self, file_path, mode="w"):
        self.file_path = StringTool.encode(file_path)
        self.mode = mode
        self._w = None
        self._w = open(self.file_path, mode=self.mode)

    def __enter__(self):
        return self

    def write(self, s):
        self._w.write(StringTool.encode(s))

    def write_line(self, s):
        self._w.write(StringTool.encode(s) + "\n")

    def write_array(self, a, connector="\t"):
        s = StringTool.join_encode(a, join_str=connector)
        self.write_line(s)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._w.close()

if __name__ == "__main__":
    with FileWriter("test鹛桑够.txt") as fw:
        fw.write("this 鹛桑够")
        fw.write(u"this 鹛桑够")
        fw.write_line("this 鹛桑够")
        fw.write_array(["this", "鹛桑够", u"鹛桑够"])
    fw2 = FileWriter(u"test鹛桑够2.txt")
    fw2.write("this 鹛桑够")
    fw2.write(u"this 鹛桑够")
    fw2.write_line("this 鹛桑够")
    fw2.write_array(["this", "鹛桑够", u"鹛桑够", 1])