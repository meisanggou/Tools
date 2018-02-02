# !/usr/bin/env python
# encoding: utf-8

import os
import MySQLdb
import ConfigParser
from datetime import date, datetime
import json

__author__ = 'meisanggou'


class DB(object):
    TIME_FORMAT = '%Y-%m-%d %H:%M:%S'
    DATE_FORMAT = '%Y-%m-%d'
    conn = None
    cursor = None
    conf_path_environ_key = "JY_DB_CONF_PATH"

    def __init__(self, conf_path=None, conf_dir=None, readonly=False, user=None, password=None):
        self.readonly = readonly
        if conf_path is None:
            if conf_dir is not None:
                conf_path = os.path.join(conf_dir, "mysql_app.conf")
            elif os.environ.get(self.conf_path_environ_key) is not None:
                conf_path = os.environ.get(self.conf_path_environ_key)
            else:
                conf_path = "mysql_app.conf"
        self.conf_path = conf_path
        self._int_app(conf_path, readonly)
        if user is not None and password is not None:
            self._db_user = user
            self._db_password = password

    @staticmethod
    def _read_conf(conf_path, readonly):
        config = ConfigParser.ConfigParser()
        config.read(conf_path)
        basic_section = "db_basic"
        host = config.get(basic_section, "host")
        db_name = config.get(basic_section, "name")
        db_port = config.getint(basic_section, "port")
        if readonly is True:
            user_section = "%s_read_user" % basic_section
        else:
            user_section = "%s_user" % basic_section
        db_user = config.get(user_section, "user")
        db_password = config.get(user_section, "password")
        return dict(host=host, db_name=db_name, db_port=db_port, db_user=db_user, db_password=db_password)

    def _int_app(self, conf_path, readonly):
        o = self._read_conf(conf_path, readonly)
        self.host = o["host"]
        self._db_name = o["db_name"]
        self._db_port = o["db_port"]
        self._db_user = o["db_user"]
        self._db_password = o["db_password"]

    @staticmethod
    def _connect(host, port, user, password, db_name):
        conn = MySQLdb.connect(host=host, port=port, user=user, passwd=password, db=db_name, charset='utf8')
        cursor = conn.cursor()
        conn.autocommit(True)
        return conn, cursor

    def connect(self):
        self.conn, self.cursor = self._connect(self.host, self._db_port, self._db_user, self._db_password, self._db_name)

    def literal(self, s):
        if not self.conn:
            self.connect()
        if isinstance(s, unicode) or isinstance(s, str):
            pass
        elif isinstance(s, dict) or isinstance(s, tuple) or isinstance(s, list):
            s = json.dumps(s)
        elif isinstance(s, set):
            s = json.dumps(list(s))
        s = self.conn.literal(s)
        return s

    @staticmethod
    def merge_where(where_value=None, where_is_none=None, where_cond=None, where_cond_args=None, prefix_value=None):
        args = []
        if where_cond is None:
            where_cond = list()
        else:
            where_cond = list(where_cond)
            if isinstance(where_cond_args, (list, tuple)):
                args.extend(where_cond_args)
        if where_value is not None:
            where_args = dict(where_value).values()
            args.extend(where_args)
            for key in dict(where_value).keys():
                where_cond.append("%s=%%s" % key)
        if isinstance(prefix_value, dict) is True:
            for key in prefix_value.keys():
                v = "%s" % prefix_value[key]
                v = v.replace("_", r"\_").replace("%", r"\%")
                v = "%s%%" % v
                where_cond.append("%s LIKE %%s" % key)
                args.append(v)
        if where_is_none is not None and len(where_is_none) > 0:
            for key in where_is_none:
                where_cond.append("%s is NULL" % key)
        return where_cond, args

    def execute(self, sql_query, args=None, freq=0, print_sql=False):
        if self.cursor is None:
            self.connect()
        if isinstance(sql_query, unicode):
            sql_query = sql_query.encode(self.conn.unicode_literal.charset)
        if args is not None:
            if isinstance(args, dict):
                sql_query = sql_query % dict((key, self.literal(item)) for key, item in args.iteritems())
            else:
                sql_query = sql_query % tuple([self.literal(item) for item in args])
        try:
            if print_sql is True:
                print(sql_query)
            handled_item = self.cursor.execute(sql_query)
        except MySQLdb.Error as error:
            print(error)
            if freq >= 3 or error.args[0] in [1054, 1064, 1146, 1065]:  # 列不存在 sql错误 表不存在 empty_query
                raise MySQLdb.Error(error)
            self.connect()
            return self.execute(sql_query=sql_query, freq=freq + 1)
        return handled_item

    def execute_multi_select(self, table_name, where_value=None, cols=None, package=True, **kwargs):
        kwargs = dict(kwargs)
        if cols is None:
            select_item = "*"
        else:
            select_item = ",".join(tuple(cols))
        select_sql = "SELECT %s FROM %s" % (select_item, table_name)
        sql_query = ""
        args = []
        if isinstance(where_value, dict):
            for item in where_value:
                value_list = where_value[item]
                for value in value_list:
                    sql_query += "{0} WHERE {1}=%s union ".format(select_sql, item)
                    args.append(value)
        sql_query = sql_query[:-7]
        order_by = kwargs.pop("order_by", None)
        order_desc = kwargs.pop("order_desc", False)
        if order_by is not None:
            if isinstance(order_by, list) or isinstance(order_by, tuple):
                sql_query += " ORDER BY %s" % ",".join(order_by)
            elif isinstance(order_by, unicode) or isinstance(order_by, str):
                sql_query += " ORDER BY %s" % order_by
            if order_desc is True:
                sql_query += " DESC"
        sql_query += ";"
        exec_result = self.execute(sql_query, args)
        if cols is not None and package is True:
            db_items = self.fetchall()
            select_items = []
            for db_item in db_items:
                r_item = dict()
                for i in range(len(cols)):
                    c_v = db_item[i]
                    if isinstance(c_v, datetime):
                        c_v = c_v.strftime(self.TIME_FORMAT)
                    elif isinstance(c_v, date):
                        c_v = c_v.strftime(self.DATE_FORMAT)
                    elif isinstance(c_v, str):
                        if c_v == "\x00":
                            c_v = False
                        elif c_v == "\x01":
                            c_v = True
                        else:
                            print(c_v)
                    r_item[cols[i]] = c_v
                select_items.append(r_item)
            return select_items
        return exec_result

    def execute_select(self, table_name, where_value=None, where_cond=None, cols=None, package=True, **kwargs):
        kwargs = dict(kwargs)
        where_is_none = kwargs.pop("where_is_none", None)
        where_cond_args = kwargs.pop("where_cond_args", None)
        prefix_value = kwargs.pop("prefix_value", None)
        where_cond, args = self.merge_where(where_value=where_value, where_cond=where_cond, where_is_none=where_is_none,
                                            where_cond_args=where_cond_args, prefix_value=prefix_value)
        if cols is None:
            select_item = "*"
        else:
            select_item = ",".join(tuple(cols))
        if len(where_cond) > 0:
            sql_query = "SELECT %s FROM %s WHERE %s" % (select_item, table_name, " AND ".join(where_cond))
        else:
            sql_query = "SELECT %s FROM %s" % (select_item, table_name)
        order_by = kwargs.pop("order_by", None)
        order_desc = kwargs.pop("order_desc", False)
        limit = kwargs.pop("limit", None)
        if order_by is not None:
            if isinstance(order_by, list) or isinstance(order_by, tuple):
                sql_query += " ORDER BY %s" % ",".join(order_by)
            elif isinstance(order_by, unicode) or isinstance(order_by, str):
                sql_query += " ORDER BY %s" % order_by
            if order_desc is True:
                sql_query += " DESC"
        if isinstance(limit, int):
            sql_query += " LIMIT %s" % limit
        sql_query += ";"
        print_sql = kwargs.get("print_sql", False)
        exec_result = self.execute(sql_query, args, print_sql=print_sql)
        if cols is not None and package is True:
            db_items = self.fetchall()
            select_items = []
            for db_item in db_items:
                r_item = dict()
                for i in range(len(cols)):
                    c_v = db_item[i]
                    if isinstance(c_v, datetime):
                        c_v = c_v.strftime(self.TIME_FORMAT)
                    elif isinstance(c_v, date):
                        c_v = c_v.strftime(self.DATE_FORMAT)
                    elif isinstance(c_v, str):
                        if c_v == "\x00":
                            c_v = False
                        elif c_v == "\x01":
                            c_v = True
                        else:
                            print(c_v)
                    r_item[cols[i]] = c_v
                select_items.append(r_item)
            return select_items
        return exec_result

    def execute_insert(self, table_name, kwargs, ignore=False):
        keys = dict(kwargs).keys()
        if ignore is True:
            sql_query = "INSERT IGNORE INTO %s (%s) VALUES (%%(%s)s);" % (table_name, ",".join(keys),
                                                                          ")s,%(".join(keys))
        else:
            sql_query = "INSERT INTO %s (%s) VALUES (%%(%s)s);" % (table_name, ",".join(keys), ")s,%(".join(keys))
        return self.execute(sql_query, args=kwargs)

    def execute_duplicate_insert(self, t_name, kwargs, u_keys=None, p1_keys=None, u_v=None):
        if isinstance(kwargs, dict) is False:
            raise TypeError()
        if u_v is None:
            u_v = []
        if isinstance(u_v, list) is False:
            raise TypeError()
        keys = kwargs.keys()
        if isinstance(u_keys, (tuple, list)) is True:
            u_v.extend(map(lambda x: "{0}=VALUES({0})".format(x), u_keys))
        if isinstance(p1_keys, (tuple, list)) is True:
            u_v.extend(map(lambda x: "{0}={0}+1".format(x), p1_keys))
        if len(u_v) <= 0:
            return self.execute_insert(t_name, kwargs)
        sql = "INSERT INTO %s (%s) VALUES (%%(%s)s) ON DUPLICATE KEY UPDATE %s;" \
              % (t_name, ",".join(keys), ")s,%(".join(keys), ",".join(u_v))
        return self.execute(sql, args=kwargs)

    def execute_insert_mul(self, table_name, cols, value_list, ignore=False):
        keys = cols
        if ignore is True:
            sql_query = "INSERT IGNORE INTO %s (%s) VALUES " % (table_name, ",".join(keys))
        else:
            sql_query = "INSERT INTO %s (%s) VALUES " % (table_name, ",".join(keys))
        if not isinstance(value_list, list):
            return -1
        if len(value_list) <= 0:
            return 0
        args = []
        for value_item in value_list:
            sql_query += "(" + ("%s," * len(value_item)).rstrip(",") + "),"
            args.extend(value_item)
        sql_query = sql_query.rstrip(",") + ";"
        return self.execute(sql_query, args=args)

    def execute_duplicate_insert_mul(self, t_name, cols, values, u_keys=None, p1_keys=None, concat_keys=None, u_v=None,
                                     print_sql=False):
        u_l = list()
        if isinstance(u_v, (tuple, list)) is True:
            u_l.extend(u_v)
        if isinstance(u_keys, (tuple, list)) is True:
            u_l.extend(map(lambda x: "{0}=VALUES({0})".format(x), u_keys))
        if isinstance(p1_keys, (tuple, list)) is True:
            u_l.extend(map(lambda x: "{0}={0}+1".format(x), p1_keys))
        if isinstance(concat_keys, (tuple, list)) is True:
            u_l.extend(map(lambda x: "{0}=concat({0}, ',', VALUES({0}))".format(x), concat_keys))
        if len(u_l) <= 0:
            return self.execute_insert_mul(t_name, cols, values)
        keys = cols
        sql_query = "INSERT INTO %s (%s) VALUES " % (t_name, ",".join(keys))
        if isinstance(values, (list, tuple)) is False:
            raise TypeError()
        if len(values) <= 0:
            return 0
        args = []
        for value_item in values:
            sql_query += "(" + ("%s," * len(value_item)).rstrip(",") + "),"
            args.extend(value_item)
        sql_query = sql_query.rstrip(",") + " ON DUPLICATE KEY UPDATE %s;" % ",".join(u_l)
        return self.execute(sql_query, args=args, print_sql=print_sql)

    def execute_update(self, table_name, update_value=None, update_value_list=None, where_value=None, where_is_none=[],
                       where_cond=None):
        if update_value_list is None:
            update_value_list = list()
        else:
            update_value_list = list(update_value_list)
        args = []
        if update_value is not None and isinstance(update_value, dict):
            args.extend(update_value.values())
            for key in update_value.keys():
                update_value_list.append("{0}=%s".format(key))
        if len(update_value_list) <= 0:
            return 0
        sql_query = "UPDATE %s SET %s WHERE " % (table_name, ",".join(update_value_list))
        if isinstance(where_cond, tuple) or isinstance(where_cond, list):
            where_cond = list(where_cond)
        else:
            where_cond = []
        if where_value is not None:
            where_args = dict(where_value).values()
            args.extend(where_args)
            for key in dict(where_value).keys():
                where_cond.append("%s=%%s" % key)
        if len(where_is_none) > 0:
            for key in where_is_none:
                where_cond.append("%s is NULL" % key)
        sql_query += " AND ".join(where_cond) + ";"
        return self.execute(sql_query, args=args)

    def execute_delete(self, table_name, where_value):
        args = dict(where_value).values()
        if len(args) <= 0:
            return 0
        sql_query = "DELETE FROM %s WHERE %s=%%s;" % (table_name, "=%s AND ".join(dict(where_value).keys()))
        return self.execute(sql_query, args)

    def execute_call(self, p_name, *args):
        sql_query = "CALL %s(" % p_name
        sql_query += ",".join(map(self.literal, args))
        sql_query += ");"
        return self.execute(sql_query)

    def create_user(self, user, password, host='localhost', db=None, readonly=False):
        l = self.execute_select("mysql.user", where_value=dict(user=user, host=host))
        if l > 0:
            return False
        c_sql = "CREATE USER %s@%s IDENTIFIED BY %s;"
        self.execute(c_sql, args=(user, host, password))
        if db is not None:
            if readonly is False:
                g_sql = "GRANT ALL ON {db}.* TO %s@%s;"
            else:
                g_sql = "GRANT SELECT ON {db}.* TO %s@%s;"
            self.execute(g_sql.format(db=db), args=(user, host))
        return True

    def root_init_conf(self, host='localhost'):
        o = self._read_conf(self.conf_path, False)
        self.create_user(o["db_user"], o["db_password"], host=host, db=o["db_name"], readonly=False)
        o = self._read_conf(self.conf_path, True)
        self.create_user(o["db_user"], o["db_password"], host=host, db=o["db_name"], readonly=True)

    def source_file(self, file_path):
        cmd = "mysql -u%s -p%s %s < %s" % (self._db_user, self._db_password, self._db_name, file_path)
        os.system(cmd)

    def table_exist(self, t_name):
        where_value = dict(TABLE_SCHEMA=self._db_name, TABLE_TYPE='BASE TABLE', TABLE_NAME=t_name)
        cols = ["TABLE_NAME", "CREATE_TIME", "TABLE_COMMENT"]
        l = self.execute_select("information_schema.TABLES", where_value=where_value, cols=cols, package=False)
        if l == 0:
            return False
        return True

    def fetchone(self):
        return self.cursor.fetchone()

    def fetchall(self):
        return self.cursor.fetchall()

    def close(self):
        if self.cursor:
            self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    db = DB()
    items = db.execute_select("zh_test", prefix_value=dict(a=r"abc_", b="%"), cols=["a"], print_sql=True)
    for item in items:
        print(item)