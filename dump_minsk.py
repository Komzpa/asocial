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

BATCH_SIZE = 25

# cursor.execute("select distinct t from vk_people, vk_friends where f=uid and city=282 and not exists(select uid from vk_people where uid=t);")

while True:
    cursor.execute("select uid from vk_people where city =282 and not exists(select * from vk_friends where f=uid) and home_phone!='';")
    friends_to_process = ([i[0] for i in cursor.fetchall()])
    if not friends_to_process:
        cursor.execute("select distinct t from vk_people, vk_friends where f=uid and city=282 and mobile_phone != '' and not exists(select uid from vk_people where uid=t) limit 10000;")
        profiles_to_process = ([i[0] for i in cursor.fetchall()])
        if profiles_to_process:
            ensure_user_profiles(profiles_to_process)
            continue
    if not friends_to_process:
        cursor.execute("select uid from vk_people where city=282 and not exists(select * from vk_friends where f=uid) limit 100;")
        friends_to_process = ([i[0] for i in cursor.fetchall()])
    print "found %s users to recheck" % len(friends_to_process)
    print "got %s new users from %s users" % (len(ensure_users_friends(friends_to_process)), len(friends_to_process))
