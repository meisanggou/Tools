#! /usr/bin/env python
# coding: utf-8

import thread
import requests
from json import dumps as json_dumps

__author__ = 'ZhouHeng'


class RequestsManager(object):
    def __init__(self, conn_error_code=-1, http_error_code=1):
        self._req = requests.session()
        self.conn_error_code = conn_error_code
        self.http_error_code = http_error_code
        self.verify_http_code = True

    @property
    def auth(self):
        return self._req.auth

    @auth.setter
    def auth(self, v):
        self._req.auth = v

    @property
    def headers(self):
        return self._req.headers

    @headers.setter
    def headers(self, v):
        self._req.headers = v

    def options(self, url, **kwargs):
        return self._req.options(url, **kwargs)

    def head(self, url, **kwargs):
        return self._req.head(url, **kwargs)

    def get(self, url, **kwargs):
        return self.request("get", url, **kwargs)

    def post(self, url, data=None, json=None, **kwargs):
        return self.request("post", url, data=data, json=json, **kwargs)

    def put(self, url, data=None, **kwargs):
        return self.request("put", url, data=data, **kwargs)

    def delete(self, url, **kwargs):
        return self.request("delete", url, **kwargs)

    def close(self):
        self._req.close()

    def request(self, method, url, **kwargs):
        as_thread = kwargs.pop("as_thread", False)
        if as_thread is True:
            return thread.start_new_thread(self.request, (method, url), kwargs)
        if "allow_redirects" not in kwargs:
            kwargs["allow_redirects"] = True
        body = kwargs.pop("body", None)
        if body is not None:
            if method == "GET":
                kwargs["params"] = body
            elif method == "POST" or method == "GET" or method == "DELETE":
                kwargs["json"] = body
        try:
            resp = self._req.request(method, url, **kwargs)
        except requests.ConnectionError as ce:
            if hasattr(ce.message, "reason") is True:
                msg = ce.message.reason
            else:
                msg = ce.message
            raise JYRequestsException(self.conn_error_code, url, message=msg, **kwargs)
        if self.verify_http_code is True and resp.status_code != 200:
            raise JYRequestsException(self.http_error_code, url, http_code=resp.status_code, **kwargs)
        return resp


class JYRequestsException(Exception):
    def __init__(self, error_type, url, **kwargs):
        self.error_type = error_type
        self.url = url
        if "http_code" in kwargs:
            self.http_code = kwargs["http_code"]
        else:
            self.http_code = 0
        if "message" in kwargs:
            self.message = str(kwargs["message"])
        else:
            self.message = ""
        self.json = None
        self.data = None
        if "json" in kwargs:
            self.json = kwargs["json"]
        if "data" in kwargs:
            self.data = kwargs["data"]

    def __str__(self):
        exp_msg = {"url": self.url, "error": self.message, "http_code": self.http_code}
        if self.data is not None:
            exp_msg["data"] = self.data
        if self.json is not None:
            exp_msg["json"] = self.json
        return json_dumps(exp_msg)
