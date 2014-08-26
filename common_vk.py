#!/usr/bin/env python
# -*- coding: utf-8 -*-

import vkontakte
import psycopg2
import time
from datetime import datetime
import StringIO
import socket
import ssl
import os
import sys
import progressbar
import json

reload(sys)
sys.setdefaultencoding("utf-8")

import personal

vk = vkontakte.API(token=personal.vk_token)

database = "dbname=gis user=gis"

a = psycopg2.connect(database)
a.autocommit = True
cursor = a.cursor()

is_online = True
# is_online = False

USER_GROUP_CACHE = {}


def get_psql_cursor():
    return cursor


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
        cursor.execute("delete from vk_people where uid in (%(profile_ids)s);insert into vk_people (uid, first_name, last_name, nickname, bdate, domain, photo_big, sex, country, city, timezone, mobile_phone, home_phone, university, faculty, graduation) values %(values)s;" % locals())
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
            cursor.execute("delete from vk_university where university = %(university)s and faculty = %(faculty)s; insert into vk_university (university, faculty, university_name, faculty_name) values (%(university)s, %(faculty)s, %(university_name)s, %(faculty_name)s);" % i)
        connected = False
        for k in ('skype', 'twitter', 'livejournal', 'instagram', 'facebook'):
            if k not in i:
                i[k] = ""
            else:
                connected = True
            i[k] = "'" + str(i[k]).replace("'", "''").strip() + "'"
        if connected:
            i["facebook"] = int("0" + i["facebook"].strip("'"))
            cursor.execute("delete from social_connections where vk = %(uid)s; insert into social_connections (skype, twitter, livejournal, instagram, facebook, vk) values (%(skype)s, %(twitter)s, %(livejournal)s, %(instagram)s, %(facebook)s, %(uid)s);" % i)


def insert_user_friends(user, friends):
    """
    user and friends are id's
    """
    if not friends:
        friends = [0]
    l = friends
    cursor.execute("delete from vk_friends where f = %s; insert into vk_friends (f, t) values " % (user) + ",".join(["(%s, %s)" % (user, v) for v in l]) + ";")


def _convert_post(post):
    newpost = {}
    newpost["id"] = post["id"]
    newpost["from_id"] = post["from_id"]
    newpost["owner_id"] = post["owner_id"]
    newpost["date"] = datetime.fromtimestamp(post["date"])
    newpost["text"] = post["text"]
    newpost["reply_count"] = post["comments"]["count"]
    newpost["likes_count"] = post["likes"]["count"]
    newpost["share_count"] = post["reposts"]["count"]
    newpost["has_attachments"] = "attachment" in post
    newpost["is_repost"] = "copy_history" in post
    return newpost


def insert_post(post):
    cursor.execute("delete from vk_wall where id=%(id)s and date=%(date)s; insert into vk_wall (id, from_id, owner_id, date, text, reply_count, likes_count, has_attachments, is_repost) values (%(id)s, %(from_id)s, %(owner_id)s, %(date)s, %(text)s, %(reply_count)s, %(likes_count)s, %(has_attachments)s, %(is_repost)s);", post)


def get_user_posts(user, all_posts=False):
    if not user:
        return []
    num_posts = 0
    offset = 0
    posts = []
    err_count = 0
    while True:
        try:
            resp = vk.get('wall.get', owner_id=user, count=100, offset=offset, filter="owner", v=5.7)
            count = resp['count']
            offset += 100
            part = [_convert_post(t) for t in resp["items"]]
            [insert_post(b) for b in part]
            posts += part
            if not all_posts or len(posts) >= count:
                return posts
            err_count = 0
        except (vkontakte.api.VKError):
            err_count += 1
            if err_count < 10:
                continue
            return posts
        except (socket.error):
            err_count += 1
            if err_count < 10:
                time.sleep(0.3)
                continue
            return posts
        except (UnicodeDecodeError):
            return posts


def ensure_user_profiles(friends):
    if not friends:
        return
    if not is_online:
        return
    friends = set(friends)

    cursor.execute('select uid from vk_people where uid in (' + ",".join([str(int(uid)) for uid in friends]) + ");")
    friends_in_db = set([i[0] for i in cursor])
    frieds_to_fetch = friends - friends_in_db
    # print len(frieds_to_fetch)
    if frieds_to_fetch:
        # print "fetching %s profiles" % len(frieds_to_fetch)
        frieds_to_fetch = list(frieds_to_fetch)
        frieds_to_fetch.sort()
        size = 200
        while frieds_to_fetch:
            current_friends = frieds_to_fetch[:size]
            try:
                response = vk.get('users.get', fields='uid,first_name,last_name,nickname,sex,bdate,city,country,timezone,photo_big,domain,rate,contacts,education,connections', uids=",".join([str(i) for i in current_friends]))
                insert_user_profiles(response)
            except (vkontakte.api.VKError):
                size /= 1.5
                size = max(int(size), 150)
                continue
            except (socket.error, ssl.SSLError, socket.gaierror):
                continue
            size = min(1000, size + 1)
            frieds_to_fetch = frieds_to_fetch[size:]


def get_user_profiles(friends, access_api=True):
    if not friends:
        return []
    if access_api:
        ensure_user_profiles(friends)
    cursor.execute('select * from vk_people where uid in (' + ",".join([str(int(uid)) for uid in friends]) + ");")
    names = [q[0] for q in cursor.description]
    profiles = [dict(map(None, names, row)) for row in cursor]
    return profiles


def get_group_info(gid):
    if not gid:
        return {}
    profiles = []
    while not profiles:
        cursor.execute('select * from vk_groups where gid = %s;', (gid,))
        names = [q[0] for q in cursor.description]
        profiles = [dict(map(None, names, row)) for row in cursor]
        # print profiles
        if (not profiles) and is_online:
            try:
                groups = vk.get("groups.getById", group_id=gid, v=5.12)[0]
                cursor.execute('delete from vk_groups where gid = %(id)s; insert into vk_groups (gid, name, screen_name, is_closed) values (%(id)s, %(name)s, %(screen_name)s, %(is_closed)s)', groups)
                get_group_members(gid, inexact=True, always_fetch=True)
            except (vkontakte.api.VKError), e:
                print e
                time.sleep(0.3)
                continue
            except (socket.error, ssl.SSLError, socket.gaierror):
                continue
        elif not profiles and not is_online:
            profiles = [{'name': 'asocial: OFFLINE_UNAVAILABLE', 'user_count': -1}]
    return profiles[0]

        #(gid, name, screen_name, is_closed, user_count)

groups_fetched = set()


def get_users_groups(users, inexact=False):
    if not users:
        return {}
    # print "getting %s user groups"% len(users)
    users = list(users)
    # users.sort()
    groups = {}
    users_to_fetch = []
    for user in users:
        if user in USER_GROUP_CACHE:
            groups[user] = USER_GROUP_CACHE[user][:]

        else:
            users_to_fetch.append(user)
    ram_only_query = True
    if users_to_fetch:
        ram_only_query = False
        __users_from_ram = len(groups)
        __pre_pg_time = time.time()

        cursor.execute('set enable_seqscan to off;')

        cursor.execute('select uid, gid from vk_memberships where uid in (%s);' % ",".join([str(int(user)) for user in users_to_fetch]))

        __users_from_pg = 0
        __pg_rows = 0
        for i in cursor:
            __pg_rows += 1
            if i[0] not in groups:
                groups[i[0]] = [i[1]]
                __users_from_pg += 1
            else:
                groups[i[0]].append(i[1])
        users_to_fetch = []
        cursor.execute('set enable_seqscan to on;')
        for user in users:
            if (user not in groups) or ((0 not in groups[user]) and (-1 not in groups[user])):
                users_to_fetch.append(user)
                groups[user] = []  # get_user_groups(user, skip_pg=True)

        size = 24
        print 'user groups taken from ram: %s, from postgres: %s (%s rows in %ss), to fetch from web: %s, total in ram cache: %s' % (__users_from_ram, __users_from_pg, __pg_rows, time.time() - __pre_pg_time, len(users_to_fetch), len(USER_GROUP_CACHE))

        if users_to_fetch:
            __max_progress = len(users_to_fetch)
            progress = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ',
                                                        progressbar.Bar(marker=">", left='[', right=']'), ' ', progressbar.ETA()], maxval=len(users_to_fetch)).start()

        rsp_restructured = {}
        while users_to_fetch and not inexact and is_online:
            current_friends = users_to_fetch[:size]
            try:
                query = 'return [' + ", ".join(['{uid: %s, l: API.groups.get({uid: %s, filter: "groups,publics", count: 1000}) }' % (friend, friend) for friend in current_friends]) + '];'
                response = vk.get('execute', code=query)

                for fs in response:
                    # insert_user_groups(fs['uid'], fs['l'])
                    # if len(current_friends) != len(response):
                        # print 'ALARM!!', len(current_friends), len(response)
                    rsp_restructured[fs['uid']] = []
                    if fs['l']:
                        if 0 in fs['l']:
                            fs['l'].remove(0)
                        # insert_user_groups(fs['uid'], fs['l'] + [-1])
                        if fs['l'] not in groups[fs['uid']]:
                            groups[fs['uid']].extend(fs['l'])
                            rsp_restructured[fs['uid']].extend(fs['l'] + [-1])
                    else:
                        fs['l'] = [0]
                        groups[fs['uid']].extend([0])
                        # insert_user_groups(fs['uid'], [0])
                        rsp_restructured[fs['uid']].extend([0])
                    if not rsp_restructured[fs['uid']]:
                        del rsp_restructured[fs['uid']]
                    # if groups[fs['uid']]:
                        # rsp_restructured[fs['uid']] = groups[fs['uid']]
                if len(rsp_restructured) > 5000:
                    insert_groups_members(rsp_restructured)
                    rsp_restructured = {}
            except (vkontakte.api.VKError), e:
                print e
                # size -= 1
                # if size < 1:
                    # size = 1
                time.sleep(0.3)
                continue
            except (socket.error, ssl.SSLError, socket.gaierror):
                continue
            users_to_fetch = users_to_fetch[size:]
            progress.update(__max_progress - len(users_to_fetch))
        if rsp_restructured:
            insert_groups_members(rsp_restructured)
            progress.finish()
    for k, v in groups.iteritems():
        if not ram_only_query:
            USER_GROUP_CACHE[k] = list(set(v))
        while -1 in v:
            v.remove(-1)

    return groups


def get_user_groups(user, inexact=False, skip_pg=False):
    groups = []
    if not is_online:
        inexact = True

    if not skip_pg:
        # ask postgres
        cursor.execute('select gid from vk_memberships where uid=%s;', (user,))
        groups = [i[0] for i in cursor]

    if (inexact and groups) or (0 in groups) or (-1 in groups):
        # if 0 in groups:
            # groups.remove(0)
        if -1 in groups:
            groups.remove(-1)
        return groups

    groups = []

    while is_online:
        try:
            groups = vk.get("groups.get", user_id=user, filter="groups,publics", count=1000, v=5.12)["items"]
            insert_user_groups(user, groups)
            # for group in groups:
                # if group not in groups_fetched:
                    # print "group", group
                    # print group, get_group_info(group)
                    # groups_fetched.add(group)
                    # insert_group_members(group, [user])
            insert_group_members(-1, [user])
            break
        except (vkontakte.api.VKError), e:

            if e.code == 260:
                insert_group_members(0, [user])
                return []
            # print e
            time.sleep(0.5)
            continue
        except (socket.error, ssl.SSLError, socket.gaierror):
            continue
    return groups


def get_user_friends(user):
    # ask postgres
    cursor.execute('select t from vk_friends where f=%s;', (user,))
    friends = [i[0] for i in cursor]

    # if fails, get full list from vk
    while is_online:
        if not friends:
            try:
                friends = vk.get('friends.get', uid=user)
                insert_user_friends(user, friends)
            except (vkontakte.api.VKError), e:
                friends = []
                if e.code == 15:  # 'Access denied: user deactivated'
                    insert_user_friends(user, friends)

            except (socket.error, ssl.SSLError, socket.gaierror):
                continue
        break
    if friends == [0]:
        friends = []
    return friends


def get_user_followers(user):
    if is_online:
        return vk.get("subscriptions.getFollowers", uid=user, count=1000)["users"]
    else:
        return []


def get_user_subscriptions(user):
    if is_online:
        return vk.get("subscriptions.get", uid=user, count=1000)["users"]
    else:
        return []


def search_users(q):
    if is_online:
        return [i['uid'] for i in vk.get("users.search", company=q, count=1000)[1:]]
    else:
        return []


def insert_group_members(gid, members):
    if members:
        cursor.execute("delete from vk_memberships where gid = %s and uid in (%s); insert into vk_memberships (gid, uid) values " % (gid, ",".join([str(i) for i in members])) + ",".join(["(%s, %s)" % (gid, v) for v in members]) + ";")


def insert_groups_members(groups):
    if groups:
        # deleter = []
        inserter = []
        for k, v in groups.iteritems():
            # deleter.append("(uid = %s and gid in (%s))"%(k, ",".join([str(p) for p in v])))
            for p in v:
                inserter.append("%s\t%s" % (p, k))
        # deleter = " or ".join(deleter)
        inserter = "\n".join(inserter)
        # print "delete from vk_memberships where " + deleter + "; insert into vk_memberships (gid, uid) values" + inserter+ ";"
        # cursor.execute("delete from vk_memberships where " + deleter +";")
        # cursor.execute("insert into vk_memberships (gid, uid) values" + inserter+ ";")
        cursor.copy_from(StringIO.StringIO(inserter), 'vk_memberships', columns=('gid', 'uid'))


def insert_user_groups(user, groups):
    if groups:
        cursor.execute("insert into vk_memberships (gid, uid) values " + ",".join(["(%s, %s)" % (gid, user) for gid in groups]) + ";")


def get_group_members(gid, inexact=False, always_fetch=False):

    if not always_fetch or not is_online:
        cursor.execute('select uid from vk_memberships where gid = %s;', (gid,))
        members = [i[0] for i in cursor]
        cursor.execute('select user_count from vk_groups where gid = %s;', (gid,))
        count = [i[0] for i in cursor]
        if not is_online or (count and members and (count[0] <= len(members) or inexact)):
            return members

    count = 0
    offset = 0
    members = []
    while offset <= count:
        try:
            resp = vk.get("groups.getMembers", gid=gid, offset=offset)
            count = resp["count"]
            members += resp["users"]
            offset += 1000
            if inexact and offset <= count:
                break
        except (vkontakte.api.VKError), e:
            if e.code == 203:
                break
            time.sleep(0.3)
        except (socket.error, ssl.SSLError, socket.gaierror):
            continue
    print 'count', count
    cursor.execute('update vk_groups set user_count = %s where gid = %s;', (count, gid))
    insert_group_members(gid, members)
    return members


def ensure_users_friends(users):

    # TODO: filter users by postgres not to get lists twice
    size = 25
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
        except (vkontakte.api.VKError):
            time.sleep(0.3)
            size /= 1.5
            size = max(int(size), 10)
            continue
        except (socket.error, ssl.SSLError, socket.gaierror):
            continue
        size = min(1000, size + 1)
        frieds_to_fetch = frieds_to_fetch[size:]
    return list(all_friends)
