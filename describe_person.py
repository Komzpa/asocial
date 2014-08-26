#!/usr/bin/env python
# -*- coding: utf-8 -*-

import common_vk
from common_vk import *
# common_vk.is_online=False
import pprint
import time
import socket
import ssl
import os
import random
import sys
import json
import progressbar
from collections import Counter

reload(sys)
sys.setdefaultencoding("utf-8")

stat_cache = {}


def get_stats(user, deep=0, group=False, followers=False, get_groups=True):
    global stat_cache
    if not deep and user in stat_cache:
        return stat_cache[user]
    if not group:
        friends_ids = get_user_friends(user)
        if followers:
            friends_ids += get_user_followers(user)
            friends_ids += get_user_subscriptions(user)
    if group:
        friends_ids = get_group_members(user)

    friends_ids.sort()
    num = 0
    random.seed(0)
    if len(friends_ids) > 10000000:
        friends_ids = random.sample(friends_ids, 10000000)
        friends_ids.sort()

    friends = get_user_profiles(friends_ids, True)

    deep = max(0, deep - 1)

    if get_groups:
        friends_groups = get_users_groups(friends_ids)

    if deep and get_groups and len(friends_ids) < 1000:
        further_friends = Counter()
        for k, v in friends_groups.iteritems():
            if 0 in v:
                further_friends.update(get_user_friends(k))
        get_users_groups([i[0] for i in further_friends.most_common() if i[1] > 0])
        # ensure_user_profiles([i[0] for i in further_friends.most_common() if i[1] > 0])


    friends.reverse()

    stats = {
        "countries": {},
        "cities": {},
        "years": {},
        "graduation": {},
        "faculty": {},
        "university": {},
        "sex": {},
        "groups": {},
        "friends": {},
        "graduated": 0,
    }

    if followers and friends:
        progress = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ',
                                                    progressbar.Bar(marker=">", left='[', right=']'), ' ', progressbar.ETA()])
        friend_iter = progress(friends)
    else:
        friend_iter = friends
    stats["friends"] = dict([(i['uid'], 1) for i in friends])
    for friend in friend_iter:
        num += 1
        # if deep:
            # print num, '/', len(friends), friend['uid'], friend['first_name'], friend['last_name']

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
        groups = []

        if get_groups:
            groups = friends_groups[friend['uid']]
            for group in groups:
                stats["groups"][group] = stats["groups"].get(group, 0) + 1

        if friend['bdate']:
            if (friend['bdate'].year > 1940) or (not deep and friend['bdate'].year > 1200):
                year = friend['bdate'].year
        if deep:
            if (0 in groups) or (not year):
                st = get_stats(friend["uid"], deep, get_groups=(0 in groups))
                u_grps = st['groups'].copy()
                if 0 in u_grps:
                    del u_grps[0]
                if u_grps:
                    max_group = max(u_grps.values())
                    for group in u_grps.keys():
                        stats["groups"][group] = stats["groups"].get(group, 0) + 1. * u_grps[group] / max_group
                us = st["years"]
                if us:
                    year = max(us, key=us.get)

        if year:
            stats["years"][year] = stats["years"].get(year, 0) + 1

    for group in stats["groups"].keys():
        if stats["groups"][group] <= 1.5:
            del stats["groups"][group]

    groups = stats["groups"].keys()
    groups.sort(key=stats["groups"].get)
    #for group in groups[-15:]:
        #get_group_info(group)

    stat_cache[user] = stats

    return stats


if __name__ == '__main__':
    try:
        passed_id = int(sys.argv[1])
    except:
        print "usage: " + sys.argv[0] + " id"
        print "positive ids mean people, negative ids mean groups"
        exit(1)

    if passed_id > 0:
        stats = get_stats(passed_id, 2, followers=True)
        pprint.pprint(stats)
    else:
        stats = get_stats(-passed_id, 2, group=True, followers=True)
        pprint.pprint(stats)
    groups = stats["groups"].keys()
    groups.sort(key=stats["groups"].get)
    groups.reverse()

    people_like = {}
    for group in groups[:150]:
        if group:
            group_info = get_group_info(group)
            print 'http://vk.com/club%s' % group, group_info['name'], stats["groups"][group], group_info['user_count']
