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

def get_stats(user, deep=0, group=False, followers=False):
    if not group:
        friends = get_user_friends(user)
        if followers:
            friends += get_user_followers(user)
            friends += get_user_subscriptions(user)
    if group:
        friends = get_group_members(64774126)
    friends = get_user_profiles(friends)
    deep = max(0, deep - 1)
    stats = {
        "countries": {},
        "cities": {},
        "years": {},
        "graduation": {},
        "faculty": {},
        "university": {},
        "sex": {},
        "graduated": 0,
    }
    num = 0
    for friend in friends:
        num += 1
        if deep:
            print num, '/', len(friends), friend['uid']
        if friend['city']:
            stats["cities"][friend['city']] = stats["cities"].get(friend['city'], 0) + 1
        if friend['country']:
            stats["countries"][friend['country']] = stats["countries"].get(friend['country'], 0) + 1
        if friend['sex']:
            stats["sex"][friend['sex']] = stats["sex"].get(friend['sex'], 0) + 1
        if friend['university']:
            stats["university"][friend['university']] = stats["university"].get(friend['university'], 0) + 1
        if friend['faculty']:
            stats["faculty"][friend['faculty']] = stats["faculty"].get(friend['faculty'], 0) + 1
        year = 0
        if friend["graduation"]:
            stats["graduation"][friend['graduation']] = stats["graduation"].get(friend['graduation'], 0) + 1
            if not deep:
                year = friend['graduation'] - 23
            stats["graduated"] += 1
        if friend['bdate']:
            if (friend['bdate'].year > 1940) or (not deep and friend['bdate'].year > 1200):
                year = friend['bdate'].year
        if deep and not year:
            us = get_stats(friend["uid"], deep)["years"]
            if us:
                year = max(us, key=us.get)
        if year:
            stats["years"][year] = stats["years"].get(year, 0) + 1
    return stats

try:
    uid = sys.argv[1]
except:
    print "usage: " + sys.argv[0] + " id_of_interesting_user"
    exit(1)
pprint.pprint(get_stats(uid, 0, followers=True))
pprint.pprint(get_stats(uid, 1, followers=True))
pprint.pprint(get_stats(uid, 2, followers=True))

