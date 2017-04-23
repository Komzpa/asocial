drop table if exists vk_friends;
create table vk_friends (f int, t int);
create index on vk_friends (f);
create table vk_people(
    uid int primary key,

    first_name text,
    last_name text,
    nickname text,

    bdate date,
    domain text,
    photo_big text,
    sex smallint,

    country bigint,
    city bigint,
    timezone smallint,

    mobile_phone text,
    home_phone text,

    university bigint,
    faculty bigint,
    graduation smallint
);

create table vk_university(
    university bigint,
    university_name text,
    faculty bigint,
    faculty_name text
);

create table social_connections(
    skype text,
    twitter text,
    livejournal text,
    instagram text,
    facebook bigint,
    vk int
);

create table social_mbti(
    vk_id int,
    tim smallint
);

create table vk_wall(
    id int,
    from_id int,
    owner_id int,
    date timestamp,
    text text,
    reply_count smallint,
    likes_count smallint,
    has_attachments boolean,
    is_repost boolean
);

create table vk_memberships(
    uid int,
    gid int
);

create index on vk_memberships (uid);
create index on vk_memberships (gid);

create table vk_groups(
    gid int primary key,
    name text,
    screen_name text,
    is_closed smallint,
    user_count int
);




create table humans_23andme (
    hid text primary key,
    first_name text,
    last_name text,
    profile_image_url text,
    is_open_sharing boolean,
    sex text
);

drop table if exists relatives_23andme;
create table relatives_23andme (
    hid1 text,
    hid2 text,
    percentage float,
    label text
);

create unique index on relatives_23andme (hid1, hid2);

curl 'https://you.23andme.com/tools/relatives/ajax/?filter%5Bqueries%5D%5B%5D=good&hide_anonymous_toggle=true&offset=0&limit=25' -H 'pragma: no-cache' -H 'x-newrelic-id: UwAFVF5aGwoBU1JRBwU=' -H 'accept-encoding: gzip, deflate, sdch, br' -H 'x-requested-with: XMLHttpRequest' -H 'accept-language: be-BY,be;q=0.8,en-US;q=0.6,en;q=0.4,ru;q=0.2' -H 'user-agent: Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/57.0.2987.133 Safari/537.36' -H 'accept: application/json, text/javascript, */*; q=0.01' -H 'cache-control: no-cache' -H 'authority: you.23andme.com' -H 'cookie: optimizelyEndUserId=oeu1473519714856r0.5632601399623962; sc_affiliate_block=1; __cfduid=dc88b351bbde4bfdf2be1084427114f161482046550; __ibxl=1; BVBRANDID=0674d06a-0918-46c6-a0ac-66d304c58b81; cart_count.en-gb=1; cart_count.en-eu=1; __scN=1; cart_count.en-int=1; cookies_notice=True; username="fcfxUIIoQdcrAssHLMQghiZT20-PLBv4_Nfhe3UFeLk="; __CT_Data=gpv=36&apv_9654_www09=35&apv_9656_www09=1; WRUID=0; uuid=54737517bc65487da5907e39535f6c67; optimizelySegments=%7B%22171937847%22%3A%22search%22%2C%22172021755%22%3A%22false%22%2C%22172166249%22%3A%22referral%22%2C%22172171530%22%3A%22false%22%2C%22172196495%22%3A%22gc%22%2C%22172235154%22%3A%22gc%22%2C%22288245409%22%3A%22none%22%2C%22288246438%22%3A%22migration%22%7D; optimizelyBuckets=%7B%227893900010%22%3A%227865401064%22%2C%228067650891%22%3A%228067852659%22%7D; __utma=172634208.1269580301.1482046554.1492904003.1492909592.27; __utmc=172634208; __utmz=172634208.1492904003.26.13.utmcsr=you.23andme.com|utmccn=(referral)|utmcmd=referral|utmcct=/tools/compare/match/; __utmv=172634208.%2FLogged-in%2Fgenotyped_user%2Fpgs_ancestry; csrftoken=Cp9BjCVlnXxAY82qFPm0ptsoCyZ5ViAQ; _ga=GA1.2.1269580301.1482046554; sessionid=0y1g107fewm6fwhrjxjneesezuueqakq; _gali=controls-view' -H 'referer: https://you.23andme.com/tools/relatives/' --compressed


