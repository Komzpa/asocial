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
