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
reload(sys)
sys.setdefaultencoding("utf-8")

stat_cache = {}


def get_stats(user, deep=0, group=False, followers=False, get_groups=True):
    global stat_cache
    if not deep and user in stat_cache:
        return stat_cache[user]
    if not group:
        friends = get_user_friends(user)
        if followers:
            friends += get_user_followers(user)
            friends += get_user_subscriptions(user)
    if group:
        friends = get_group_members(user)

    friends.sort()
    num = 0
    random.seed(0)
    if len(friends) > 10000:
        friends = random.sample(friends, 10000)
        friends.sort()
    friends = get_user_profiles(friends, False)
    friends.reverse()

    deep = max(0, deep - 1)
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

    if followers:
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
            groups = get_user_groups(friend['uid'])
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
    #        for f_uid in st['friends'].keys():
    #            stats['friends'][f_uid] = stats["friends"].get(f_uid, 0) + 1. * st['friends'][f_uid]
                us = st["years"]
                if us:
                    year = max(us, key=us.get)

        if year:
            stats["years"][year] = stats["years"].get(year, 0) + 1

    for group in stats["groups"].keys():
        if stats["groups"][group] < 2:  # (0.005 * len(friends) + 1):
            del stats["groups"][group]

    groups = stats["groups"].keys()
    groups.sort(key=stats["groups"].get)
    for group in groups[-15:]:
        get_group_info(group)

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

            if group_info['user_count'] and ((1. * stats["groups"][group] / max(group_info['user_count'], 1)) > 1e-4):
                for uid in get_group_members(group, True):
                    people_like[uid] = people_like.get(uid, 0) + 1. * stats["groups"][group] / max(group_info['user_count'], 1)

    if passed_id < 0:
        droprefs = get_group_members(-passed_id, True)
    else:
        droprefs = get_user_friends(passed_id)

    for uid in droprefs:
        if uid in people_like:
            del people_like[uid]

    people = people_like.keys()
    people.sort(key=people_like.get)
    people.reverse()

    for person in people[:150]:
        profile = get_user_profiles([person])
        if profile:
            profile = profile[0]
            print 'http://vk.com/id%s' % person, profile["first_name"], profile["last_name"], people_like[person], profile['city']

    friends_like = stats['friends']

    for uid in droprefs:
        if uid in friends_like:
            del friends_like[uid]

    friends = friends_like.keys()
    friends.sort(key=friends_like.get)
    friends.reverse()

    for person in friends[:150]:
        profile = get_user_profiles([person])
        if profile:
            profile = profile[0]
            print 'http://vk.com/id%s' % person, profile["first_name"], profile["last_name"], friends_like[person], profile['city']
