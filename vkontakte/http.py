# coding: utf-8
from __future__ import with_statement
from contextlib import closing
import httplib
import time

# urllib2 doesn't support timeouts for python 2.5 so
# custom function is used for making http requests

conn_cache = {}


def post(url, data, headers, timeout, secure=False):
    host_port = url.split('/')[2]
    timeout_set = False
    connection = httplib.HTTPSConnection if secure else httplib.HTTPConnection
    if host_port in conn_cache:
        connection = conn_cache[host_port]
        timeout_set = True
    else:
        connection = connection(host_port, timeout=timeout)
        timeout_set = True
        conn_cache[host_port] = connection
    try:
        headers['Connection'] = 'keep-alive'
        connection.request("POST", url, data, headers)
        response = connection.getresponse()
        read = response.read()
        return (response.status, read)
    except (httplib.CannotSendRequest, httplib.BadStatusLine, httplib.ResponseNotReady):
        del conn_cache[host_port]
        return post(url, data, headers, timeout, secure)
