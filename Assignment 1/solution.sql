-- DDL

CREATE DATABASE imdb_staging;
CREATE DATABASE imdb;

-- USERS TABLE (result: imdb.users)


CREATE TABLE imdb_staging.users  
(
    user_id string,
    first_name string,
    last_name string,
    birth_date string,
    country string,
    registration_date string
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE; 

LOAD DATA INPATH 'hdfs:///user/hive/warehouse/movies/dataset/users.csv' INTO TABLE imdb_staging.users;

set hive.exec.dynamic.partition = true;
set hive.exec.dynamic.partition.mode = nonstrict;
set hive.exec.max.dynamic.partitions = 10000; 
set hive.exec.max.dynamic.partitions.pernode = 1000;


CREATE EXTERNAL TABLE imdb.users  
(
    user_id int,
    first_name string,
    last_name string,
    birth_date date,
    registration_date date
) 
PARTITIONED BY (country string, registration_year int)
STORED AS Parquet;

insert into imdb.users
partition(country, registration_year)
select
    cast(u.user_id as int) user_id,
    u.first_name first_name,
    u.last_name last_name,
    to_date(from_unixtime(unix_timestamp(u.birth_date, 'MM/dd/yyyy'))) birth_date,
    u.country country,
    to_date(from_unixtime(unix_timestamp(u.registration_date, 'MM/dd/yyyy'))) registration_date,
    year(to_date(from_unixtime(unix_timestamp(u.registration_date, 'MM/dd/yyyy')))) registration_year
from imdb_staging.users u;

-- RATINGS TABLE (result: imdb.ratings)

CREATE TABLE imdb_staging.ratings  
(
    user_id string, 
    movie_id string, 
    rating string, 
    created_at string
)  
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE; 

LOAD DATA INPATH 'hdfs:///user/hive/warehouse/movies/dataset/ratings.csv' INTO TABLE imdb_staging.ratings;

CREATE EXTERNAL TABLE imdb.ratings  
(
    user_id int, 
    movie_id int, 
    rating double, 
    created_at timestamp
)  
STORED AS Parquet;

insert into imdb.ratings
select
    cast(r.user_id as int) user_id,
    cast(r.movie_id as int) movie_id,
    cast(r.rating as double) rating,
    from_unixtime(unix_timestamp(r.created_at, 'MM/dd/yyyy hh:mm:ss')) created_at
from imdb_staging.ratings r;

-- MOVIES TABLE (result: imdb.movies && imdb.genres)

CREATE TABLE imdb_staging.movies 
(
    movie_id string, 
    title string, 
    genres string
) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE;

LOAD DATA INPATH 'hdfs:///user/hive/warehouse/movies/dataset/movies.csv' INTO TABLE imdb_staging.movies;


CREATE TABLE imdb_staging.unique_genres(
    name string
);

WITH genre_rows as (
        SELECT explode(split(m.genres, '\\|')) genre
        from imdb_staging.movies m
    ) 
INSERT into imdb_staging.unique_genres
    select DISTINCT 
        case when genre = '(no genres listed)' 
        then NUll else genre end as genre
    FROM genre_rows;

CREATE EXTERNAL TABLE imdb.genres(
    id int,
    name STRING
)
STORED AS PARQUET;

INSERT into imdb.genres
select 
    row_number() over() as id,
    name 
from imdb_staging.unique_genres;

  
create external table imdb_staging.movies_v1(
    movie_id int,
    title string,
    year int,
    genres array<int> 
) 
stored as parquet;
  
-- this is the original insert into imdb.movies
-- the select statement works fine but when you insert it into the
-- it fills the title column with nulls for some reason

with movies_v2 as (
    with movies_v1 as (    
       select
         cast(movie_id as int) movie_id,
         case 
             when title like '%: The ______' THEN concat(substr(title, length(title)-9, 4),
                 split(title,'[":"]')[0])
             when title like '%: A ______' THEN concat(substr(title, length(title)-7, 2),
                 split(title,'[":"]')[0])
             when title like '%: An ______' THEN concat(substr(title, length(title)-8, 3),
                 split(title,'[":"]')[0])
             when title like '%: Les ______' THEN concat(substr(title, length(title)-9, 4),
                 split(title,'[":"]')[0])
             when title like '%: El ______' THEN concat(substr(title, length(title)-8, 3),
                 split(title,'[":"]')[0])
             when title like '%: La ______' THEN concat(substr(title, length(title)-8, 3),
                 split(title,'[":"]')[0])    
             else substr(title, 1, length(title) -7)
         end as title,
         cast(substr(title, length(title) -4, 4) as int) as year,
         split(genres, '\\|') genres
        from imdb_staging.movies
    ) 
    select movie_id, title, year, genre
    from movies_v1
    lateral view explode(genres) abc as genre 
) 
insert into imdb_staging.movies_v1
select 
    m.movie_id as movie_id,
    m.title as title, 
    m.year as year,
    collect_list(g.id) as genres
from movies_v2 m
left outer join imdb.genres g on m.genre = g.name
group by m.movie_id, m.title, m.year;


SELECT * from imdb_staging.movies_v1;

create external table imdb.movies(
    movie_id int,
    title string,
    year int,
    genres array<int> 
) 
stored as parquet;

INSERT INTO imdb.movies
SELECT * from imdb_staging.movies_v1;


-- TAGS TABLE (result: imdb.tags)

CREATE TABLE imdb_staging.tags  
(
    user_id string, 
    movie_id string, 
    tag string, 
    created_at string
)  
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ',' 
STORED AS TEXTFILE; 

LOAD DATA INPATH 'hdfs:///user/hive/warehouse/movies/dataset/tags.csv' INTO TABLE imdb_staging.tags;

create external table imdb.tags(
    user_id int, 
    movie_id string, 
    tags array<string>, 
    created_at timestamp
) 
stored as parquet;

insert into imdb.tags
select 
    cast(user_id as int) user_id,
    movie_id,
    collect_set(tag) tags,
    from_unixtime(unix_timestamp(created_at, 'MM/dd/yyyy hh:mm')) created_at
from imdb_staging.tags
group by user_id, movie_id, created_at;

create external table imdb.tags_raw(
    user_id string, 
    movie_id string, 
    tag string, 
    created_at string
) 
stored as parquet;
    
insert into imdb.tags_raw
select * from imdb_staging.tags;
    
    
    
    
-- PROBLEMS

-- 1

SELECT 
    r.movie_id movie_id, 
    year(r.created_at) year,
    avg(r.rating) avg_rating
from imdb.ratings r
group by r.movie_id, year(r.created_at)
ORDER BY r.movie_id, year(r.created_at);

--2 

select 
    m.title title,
    count(*) review_cnt,
    avg(r.rating) avg_rating
from imdb.ratings r
join imdb.movies m on r.movie_id = m.movie_id
group by m.title
having count(*) >= 100 and avg(r.rating) > 4;

--3

SELECT count(*)
from imdb.users u
LEFT OUTER JOIN imdb.ratings r 
on u.user_id = r.user_id
where r.user_id is NULL;
    
-- 4

select 
    t.user_id user_id,
    concat_ws(u.first_name, u.last_name, ' ') user_name,
    t.movie_id movie_id,
    m.title movie_title
from imdb.tags_raw t  
join imdb.users u on cast(t.user_id as int) = u.user_id
join imdb.movies m on cast(t.movie_id as int) = m.movie_id
where t.user_id in 
    (select t1.user_id
    from imdb.tags_raw t1
    where t.created_at != t1.created_at
        and t.user_id = t1.user_id
        and t.tag = t1.tag
        and t.movie_id = t1.movie_id);
        
--5

select
    m.title movie_title,
    length(t.tags) num_tags,
    t.tags tags
from imdb.tags t 
join imdb.movies m on t.movie_id = m.movie_id
order by length(t.tags)
limit 3;

--6

select 
    m.title movie_title,
    sum(r.rating) sum_ratings,
    avg(r.rating) avg_rating
from imdb.ratings r  
join imdb.movies m on r.movie_id = m.movie_id
where m.year between 2005 and 2015
group by r.movie_id, m.title
having avg(r.rating) >= 4
order by sum(r.rating) desc
limit 10;


