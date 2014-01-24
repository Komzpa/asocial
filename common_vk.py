#!/usr/bin/env python
# -*- coding: utf-8 -*-

import vkontakte
import psycopg2
import time
import datetime
import socket
import ssl
import os
import sys
import json
reload(sys)
sys.setdefaultencoding("utf-8")

import personal

vk = vkontakte.API(token=personal.vk_token)

database = "dbname=gis user=gis"

a = psycopg2.connect(database)
a.autocommit = True
cursor = a.cursor()


def escape_date_iso8601(date, null="NULL"):
    if not date:
        return null
    bd = date.split(".")
    if len(bd) == 3:
        try:
            datetime.datetime(int(bd[2]), int(bd[1]), int(bd[0]))
            return "'%04d-%02d-%02d'" % (int(bd[2]), int(bd[1]), int(bd[0]))
        except:
            return null
    elif len(bd) == 2:
        try:
            datetime.datetime(1004, int(bd[1]), int(bd[0]))
            return "'1004-%02d-%02d'" % (int(bd[1]), int(bd[0]))
        except:
            return null
    else:
        return null

userkeys = set([])


def escape_user_profile(i, nullify=True):
    """
    Prepare user profile for database insertion.
    """
    if nullify:
        null = "NULL"
    else:
        null = ""
    ss = set(i.keys())
    if not ss.issubset(userkeys):
        userkeys.update(ss)

    i["uid"] = int(i["uid"])
    i["bdate"] = escape_date_iso8601(i.get('bdate'), null)
    i["university"] = i.get("university", null)
    i["faculty"] = i.get("faculty", null)

    i["graduation"] = i.get("graduation", null)
    if i["graduation"] != null and (int(i["graduation"]) > 32767 or int(i["graduation"]) < -1000):
        i["graduation"] = -1
    i["sex"] = i.get("sex", null)
    if i["sex"] != null and (int(i["sex"]) > 32767 or int(i["sex"]) < -1000):
        i["sex"] = -1

    i["country"] = i.get("country", null)
    i["city"] = i.get("city", null)
    i["timezone"] = i.get("timezone", null)
    if i["timezone"] != null and (int(i["timezone"]) > 32767 or int(i["timezone"]) < -1000):
        i["timezone"] = -1

    for k in ("first_name", "last_name", "nickname", "domain", "photo_big", "mobile_phone", "home_phone"):
        if k not in i:
            i[k] = null
        else:
            i[k] = "'" + i[k].replace("'", "''") + "'"
    return i


def insert_user_profiles(profiles_orig):
    if not profiles_orig:
        return
    # deduplicate uids
    profiles = dict([(profile["uid"], escape_user_profile(profile.copy())) for profile in profiles_orig])
    profile_ids = ",".join([str(i) for i in profiles.keys()])

    values = ",".join(["(%(uid)s, %(first_name)s, %(last_name)s, %(nickname)s, %(bdate)s, %(domain)s, %(photo_big)s, %(sex)s, %(country)s, %(city)s, %(timezone)s, %(mobile_phone)s,  %(home_phone)s, %(university)s, %(faculty)s, %(graduation)s)" % i for i in profiles.values()])
    values = values.encode('utf-8')
    values = unicode(values, errors='ignore')
    try:
        cursor.execute("delete from vk_people where uid in (%(profile_ids)s);insert into vk_people (uid, first_name, last_name, nickname, bdate, domain, photo_big, sex, country, city, timezone, mobile_phone, home_phone, university, faculty, graduation) values %(values)s;commit;" % locals())
    except psycopg2.DataError:
        if len(profiles_orig) > 1:
            for profile in profiles_orig:
                insert_user_profiles([profile])
        else:
            print "Error inserting profile", profiles_orig[0]

    # process faculties and universities
    for i in profiles.values():
        if "faculty_name" in i:
            for k in ("university_name", "faculty_name"):
                if k not in i:
                    i[k] = ""
                i[k] = "'" + i[k].replace("'", "''").strip() + "'"
            cursor.execute("delete from vk_university where university = %(university)s and faculty = %(faculty)s; insert into vk_university (university, faculty, university_name, faculty_name) values (%(university)s, %(faculty)s, %(university_name)s, %(faculty_name)s); commit;" % i)
        connected = False
        for k in ('skype', 'twitter', 'livejournal', 'instagram', 'facebook'):
            if k not in i:
                i[k] = ""
            else:
                connected = True
            i[k] = "'" + str(i[k]).replace("'", "''").strip() + "'"
        if connected:
            i["facebook"] = int("0" + i["facebook"].strip("'"))
            cursor.execute("delete from social_connections where vk = %(uid)s; insert into social_connections (skype, twitter, livejournal, instagram, facebook, vk) values (%(skype)s, %(twitter)s, %(livejournal)s, %(instagram)s, %(facebook)s, %(uid)s); commit;" % i)


def insert_user_friends(user, friends):
    """
    user and friends are id's
    """
    if not friends:
        friends = [0]
    l = friends
    cursor.execute("delete from vk_friends where f = %s; insert into vk_friends (f, t) values " % (user) + ",".join(["(%s, %s)" % (user, v) for v in l]) + ";commit;")


def ensure_user_profiles(friends):
    if not friends:
        return
    friends = set(friends)
    cursor.execute('select uid from vk_people where uid in (' + ",".join([str(int(uid)) for uid in friends]) + ");")
    friends_in_db = set([i[0] for i in cursor.fetchall()])
    frieds_to_fetch = friends - friends_in_db
    if frieds_to_fetch:
        print "fetching %s profiles" % len(frieds_to_fetch)
        frieds_to_fetch = list(frieds_to_fetch)
        frieds_to_fetch.sort()
        size = 500
        while frieds_to_fetch:
            current_friends = frieds_to_fetch[:size]
            try:
                response = vk.get('users.get', fields='uid,first_name,last_name,nickname,sex,bdate,city,country,timezone,photo_big,domain,rate,contacts,education,connections', uids=",".join([str(i) for i in current_friends]), order='hints')
                insert_user_profiles(response)
            except (vkontakte.api.VKError, ssl.SSLError, socket.gaierror):
                size /= 1.5
                size = max(int(size), 150)
                print "reducing limit to", size
                continue
            size = min(1000, size + 1)
            frieds_to_fetch = frieds_to_fetch[size:]


def get_user_profiles(friends):
    if not friends:
        return []
    ensure_user_profiles(friends)
    cursor.execute('select * from vk_people where uid in (' + ",".join([str(int(uid)) for uid in friends]) + ");")
    names = [q[0] for q in cursor.description]
    profiles = [dict(map(None, names, row)) for row in cursor.fetchall()]
    return profiles


def get_user_friends(user):
    # ask postgres
    cursor.execute('select t from vk_friends where f=%s;', (user,))
    friends = [i[0] for i in cursor.fetchall()]
    # if fails, get full list from vk
    if not friends:
        # try:
            # print "dumping vk for http://vk.com/id%s"%user
            # response = vk.get('friends.get', fields='uid,first_name,last_name,nickname,sex,bdate,city,country,timezone,photo_big,domain,rate,contacts,education,connections', uid=user, order='hints')
            # insert_user_profiles(response)
            # friends = [i["uid"] for i in response]
            # insert_user_friends(user, friends)
        # except (vkontakte.api.VKError, ssl.SSLError, socket.gaierror):
            ## if fails, get short list from vk
            try:
                friends = vk.get('friends.get', uid=user)
                insert_user_friends(user, friends)
                ensure_user_profiles(friends)
            except (vkontakte.api.VKError, ssl.SSLError, socket.gaierror):
                friends = []
    if friends == [0]:
        friends = []
    return friends


def get_user_followers(user):
    return vk.get("subscriptions.getFollowers", uid=user, count=1000)["users"]


def get_user_subscriptions(user):
    return vk.get("subscriptions.get", uid=user, count=1000)["users"]


def get_group_members(gid):
    members = vk.get("groups.getMembers", gid=gid)["users"]
    members += vk.get("groups.getMembers", gid=gid, offset=1000)["users"]
    members += vk.get("groups.getMembers", gid=gid, offset=2000)["users"]
    time.sleep(1)
    members += vk.get("groups.getMembers", gid=gid, offset=3000)["users"]
    members += vk.get("groups.getMembers", gid=gid, offset=4000)["users"]
    members += vk.get("groups.getMembers", gid=gid, offset=5000)["users"]
    time.sleep(1)
    members += vk.get("groups.getMembers", gid=gid, offset=6000)["users"]
    members += vk.get("groups.getMembers", gid=gid, offset=7000)["users"]
    members += vk.get("groups.getMembers", gid=gid, offset=8000)["users"]
    time.sleep(1)
    members += vk.get("groups.getMembers", gid=gid, offset=9000)["users"]
    members += vk.get("groups.getMembers", gid=gid, offset=10000)["users"]
    members += vk.get("groups.getMembers", gid=gid, offset=11000)["users"]
    time.sleep(1)
    members += vk.get("groups.getMembers", gid=gid, offset=12000)["users"]
    members += vk.get("groups.getMembers", gid=gid, offset=13000)["users"]
    members += vk.get("groups.getMembers", gid=gid, offset=14000)["users"]
    return members


def ensure_users_friends(users):
    # TODO: filter users by postgres not to get lists twice
    size = 100
    frieds_to_fetch = users
    all_friends = set()
    to_profile = set()
    while frieds_to_fetch:
        current_friends = frieds_to_fetch[:size]
        try:
            query = 'return [' + ", ".join(['{uid: %s, l: API.friends.get({uid: %s}) }' % (friend, friend) for friend in current_friends]) + '];'
            response = vk.get('execute', code=query)
            for fs in response:
                insert_user_friends(fs['uid'], fs['l'])
                if fs['l']:
                    all_friends.update(fs['l'])
                    to_profile.update(fs['l'])
            ensure_user_profiles(list(to_profile))
            to_profile = set()
        except (vkontakte.api.VKError, ssl.SSLError, socket.gaierror):
            time.sleep(0.3)
            size /= 1.5
            size = max(int(size), 10)
            print "reducing limit to", size
            continue
        size = min(1000, size + 1)
        frieds_to_fetch = frieds_to_fetch[size:]
    return list(all_friends)
