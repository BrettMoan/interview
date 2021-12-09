CREATE schema netflix;
create table netflix.shows
(
    show_id       text,
    type          text,
    title         text,
    director      text,
    "cast"          text,
    country       text,
    date_added    date,
    release_year  integer,
    rating        text,
    duration      text,
    listed_in     text,
    description   text
);

create table netflix.shows_v2
(
    show_id       text,
    type          text,
    title         text,
    director      text ARRAY,
    "cast"        text ARRAY,
    country       text ARRAY,
    date_added    date,
    release_year  integer,
    rating        text,
    duration      text,
    listed_in     text ARRAY,
    description   text
);

