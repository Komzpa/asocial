drop table if exists vk_friends;
create table vk_friends (f bigint, t bigint);
create index on vk_friends (f);
create table vk_people(
    uid bigint primary key,

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
    vk bigint
);