import logging
import re

import progressbar
import psycopg2
import requests

logging.basicConfig(level=logging.INFO)

# reload(sys)
# sys.setdefaultencoding("utf-8")

login = ''
password = ''


class meand23():
    def __init__(self, login, password):
        self.s = requests.Session()
        self.login = login
        self.password = password
        self.current_profile = None
        self.all_profiles = []
        database = "dbname=gis user=gis"
        a = psycopg2.connect(database)
        a.autocommit = True
        self.cursor = a.cursor()

        self.logon()

    def logon(self):
        logging.info('logging in')
        login_page = self.s.get('https://www.23andme.com/cas/signin/')

        payload = {
            'username': login,
            'password': password,
            'redirect': None,
            'source_flow': None,
            '__source_node__': 'start',
            '__form__': 'login',
            '__context__': re.search('name="__context__" value="(.+?)" />', login_page.text).groups()[0]
        }

        login_page = self.s.post('https://www.23andme.com/cas/signin/', data=payload)

        self.current_profile = re.search('"profile_id": "(.+?)"', login_page.text).groups()[0]
        self.all_profiles = list(set(re.findall('profile-id=(.+?)&', login_page.text)))
        self.all_profiles.insert(0, self.current_profile)
        logging.info('logged in, found profiles %s' % str(self.all_profiles))

    def change_profile(self, profile):
        if profile not in self.all_profiles:
            raise 'unknown profile %s' % profile
        if profile != self.current_profile:
            logging.info('changing profile to %s' % profile)
            self.s.get('https://you.23andme.com/switch-profile/?profile-id=%s&redirect-uri=/' % profile)
            self.current_profile = profile

    def sync_all_humans(self, ):
        for profile in self.all_profiles:
            self.change_profile(profile)

            # sync humans
            matches = []
            logging.info('syncing human profiles')
            profiles = self.s.get(
                'https://you.23andme.com/tools/relatives/ajax/?offset=0&limit=99999').json()
            for human in profiles['relatives']:
                if human['human_id']:
                    self.store_human(
                        {
                            'hid': human['human_id'],
                            'first_name': human['first_name'],
                            'last_name': human['last_name'],
                            'profile_image_url': human['img'],
                            'is_open_sharing': human['new_share_status'] == 'OPEN_SHARING',
                            'sex': human['sex']
                        }
                    )
                    self.store_relation(
                        profile,
                        human['human_id'],
                        float(human['pct'].strip('%')),
                        human['rel_alg']
                    )
                    matches.append(human['match_id'])

            # sync relations of match
            logging.info('syncing human relations')
            bar = progressbar.ProgressBar(widgets=[progressbar.Percentage(), ' ',
                                                   progressbar.Bar(marker=">", left='[', right=']'), ' ',
                                                   progressbar.ETA()])
            for match in bar(matches):
                try:
                    relations = self.s.get(
                        'https://you.23andme.com/tools/compare/match/relatives_in_common/?remote_id=%s&limit=1000000&offset=0' % match,
                        timeout=20).json()[
                        'relatives_in_common']
                    for r in relations:
                        if r['owner_ehid'] and r['remote_ehid']:
                            self.store_relation(
                                r['remote_ehid'],
                                r['owner_ehid'],
                                float(r['remote_percentage'].strip('%')),
                                r['remote_rel_alg_label']
                            )
                            self.store_relation(
                                r['local_ehid'],
                                r['owner_ehid'],
                                float(r['local_percentage'].strip('%')),
                                r['local_rel_alg_label']
                            )
                except requests.exceptions.Timeout:
                    pass

    def store_human(self, human):
        if not human['hid']:
            logging.warning('bad human row write attempt - %s' % str(human))
            return
        self.cursor.execute(
            '''insert into humans_23andme
    (hid, first_name, last_name, profile_image_url, is_open_sharing, sex) values
    (%s, %s, %s, %s, %s, %s)
    on conflict (hid) do nothing;
''', (human['hid'], human['first_name'], human['last_name'], human['profile_image_url'], human['is_open_sharing'],
      human['sex']))

    def store_relation(self, hid1, hid2, percentage, label):
        if not hid1 or not hid2:
            logging.warning('bad relative row write attempt -(%s, %s, %s, %s)' % (hid1, hid2, percentage, label))
            return
        self.cursor.execute(
            '''insert into relatives_23andme
    (hid1, hid2, percentage, label) values
    (%s, %s, %s, %s)
    on conflict (hid1, hid2) do update set percentage = EXCLUDED.percentage, label = EXCLUDED.label;
''', (hid1, hid2, percentage, label))


if __name__ == "__main__":
    meconnector = meand23(login, password)
    meconnector.sync_all_humans()
