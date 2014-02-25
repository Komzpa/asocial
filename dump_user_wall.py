#!/usr/bin/env python
# -*- coding: utf-8 -*-

from common_vk import *
import pprint
import time
import socket
import ssl
import os
import random
import sys
import json
reload(sys)
sys.setdefaultencoding("utf-8")

if __name__ == '__main__':
    try:
        uid = sys.argv[1]
    except:
        print "usage: " + sys.argv[0] + " id_of_interesting_user"
        exit(1)
    posts = get_user_posts(uid, True)
    for post in posts:
        post["date"] = post["date"].isoformat()
    print json.dumps(posts, ensure_ascii = False, indent=4, separators=(',', ': '))
