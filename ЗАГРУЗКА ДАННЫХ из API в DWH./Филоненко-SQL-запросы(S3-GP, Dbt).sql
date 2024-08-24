--PROJECT

-- 0. Включаем расширение
CREATE EXTENSION IF NOT EXISTS pxf;

--1.CLUBS

--01. Creating external table\ создаем внешнюю таблицу clubs_parquet в схеме public

DROP EXTERNAL TABLE IF EXISTS public.clubs_parquet;
CREATE EXTERNAL TABLE public.clubs_parquet (
	clubs_id text,
	clubs_name text,
	league_id text,
	league_name text,
	league_seasonid text
)
LOCATION (
	'pxf://transfermarkt/processed/20240208/clubs.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

--02. Проверяем доступ\контакт\связь в S3
select * from public.clubs_parquet;

--03. Creаting stg schema and table\Создаем схему и таблицу для загрузки данных из S3

create schema stg;
drop schema stg;


drop table stg.stg_clubs CASCADE;
create table stg.stg_clubs
(
	clubs_id text,
	clubs_name text,
	league_id text,
	league_name text,
	league_seasonid text,
	load_date date default  now()
);

--04. Проверяем создание таблицы c колонками

select * from stg.stg_clubs;

--05.Загружаем данные по клубам из S3 через внешнюю таблицу в GP
 
TRUNCATE TABLE stg.stg_clubs;

INSERT INTO stg.stg_clubs
	select
		clubs_id,
        clubs_name,
        league_id,
        league_name,
        league_seasonid
	FROM public.clubs_parquet;

--06.Проверяем загрузку данных
select * from stg.stg_clubs;


select
    cast(clubs_id as int)
    , clubs_name
    , league_id as competition_id
    , league_name as competition_name
    , cast(league_seasonid as int) as season_id
 
from stg.stg_clubs;

--2.CLUB-PROFILES

--01. Creating external table\ создаем внешнюю таблицу club_profiles в схеме public

DROP EXTERNAL TABLE IF EXISTS public.club_profiles_parquet;
CREATE EXTERNAL TABLE public.club_profiles_parquet (
	id text,
	name text,
	officialName text,
	addressLine1 text,
	addressLine2 text,
	addressLine3 text,
	stadiumName	text,
	stadiumSeats text,
	currentMarketValue text
)
LOCATION (
	'pxf://transfermarkt/processed/20240131/club_profiles.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

--02. Проверяем доступ\контакт\связь в S3
select * from public.club_profiles_parquet;

--03. Creаting stg table\Создаем таблицу для загрузки данных из S3


drop table stg.stg_club_profiles CASCADE;
create table stg.stg_club_profiles
(
	id text,
	name text,
	officialName text,
	addressLine1 text,
	addressLine2 text,
	addressLine3 text,
	stadiumName	text,
	stadiumSeats text,
	currentMarketValue text,
	load_date date default  now()
);



--select * from public.club_profiles_parquet;
--04. Проверяем создание таблицы c колонками

select * from stg.stg_club_profiles;

--05.Загружаем данные по клубам из S3 через внешнюю таблицу в GP
 
TRUNCATE TABLE stg.stg_club_profiles;

INSERT INTO stg.stg_club_profiles
	select
		id, 
		name,
	    officialName,
	    addressLine1,
	    addressLine2,
	    addressLine3,
	    stadiumName,
	    stadiumSeats,
	    currentMarketValue
	FROM public.club_profiles_parquet;

--06.Проверяем загрузку данных
select * from stg.stg_club_profiles;


--3.CLUB-PLAYERS

--01. Creating external table\ создаем внешнюю таблицу club_players в схеме public

DROP EXTERNAL TABLE IF EXISTS public.club_players_parquet;
CREATE EXTERNAL TABLE public.club_players_parquet (
	club_id text,
	player_id text
)
LOCATION (
	'pxf://transfermarkt/processed/20240131/club_players.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

--02. Проверяем доступ\контакт\связь в S3
select * from public.club_players_parquet;

--03. Creаting stg table\Создаем таблицу для загрузки данных из S3


drop table stg.stg_club_players CASCADE;
create table stg.stg_club_players
(
	club_id text,
	player_id text,
	load_date date default  now()
);



--select * from public.club_players_parquet;
--04. Проверяем создание таблицы c колонками

select * from stg.stg_club_players;

--05.Загружаем данные по клубам из S3 через внешнюю таблицу в GP
 
TRUNCATE TABLE stg.stg_club_players;

INSERT INTO stg.stg_club_players
	select
		club_id int,
	    player_id int
	FROM public.club_players_parquet;

--06.Проверяем загрузку данных
select * from stg.stg_club_players;


--4.PLAYER-PROFILES

--01. Creating external table\ создаем внешнюю таблицу player_profiles в схеме public

DROP EXTERNAL TABLE IF EXISTS public.player_profiles_parquet;
CREATE EXTERNAL TABLE public.player_profiles_parquet (
	id text,
	name text,
	fullName text,
	dateOfBirth text,
	height text,
	isRetired BOOLEAN,
	foot	text,
	placeOfBirth_country text,
	position_main text,
	club_joined text,
	club_contractExpires	text,
	agent_name text,
	description text,
	nameInHomeCountry text
)
LOCATION (
	'pxf://transfermarkt/processed/20240131/player_profiles.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

--02. Проверяем доступ\контакт\связь в S3
select * from public.player_profiles_parquet;

--03. Creаting stg table\Создаем таблицу для загрузки данных из S3


drop table stg.stg_player_profiles CASCADE;
create table stg.stg_player_profiles
(
	id text,
	name text,
	fullName text,
	dateOfBirth text,
	height text,
	isRetired BOOLEAN,
	foot	text,
	placeOfBirth_country text,
	position_main text,
	club_joined text,
	club_contractExpires	text,
	agent_name text,
	description text,
	nameInHomeCountry text,
	load_date date default  now()
);



--select * from public.player_profiles_parquet;
--04. Проверяем создание таблицы c колонками

select * from stg.stg_player_profiles;

--05.Загружаем данные по клубам из S3 через внешнюю таблицу в GP
 
TRUNCATE TABLE stg.stg_player_profiles;

INSERT INTO stg.stg_player_profiles
	select
	id,
	name,
	fullName,
	dateOfBirth,
	height,
	isRetired,
	foot,
	placeOfBirth_country,
	position_main,
	club_joined,
	club_contractExpires,
	agent_name,
	description,
	nameInHomeCountry
	FROM public.player_profiles_parquet;

--06.Проверяем загрузку данных
select * from stg.stg_player_profiles;

--5.PLAYER-OTHER-POSITIONS

--01. Creating external table\ создаем внешнюю таблицу player_other_positions в схеме public

DROP EXTERNAL TABLE IF EXISTS public.player_other_positions_parquet;
CREATE EXTERNAL TABLE public.player_other_positions_parquet (
	id text,
	position_other text
)
LOCATION (
	'pxf://transfermarkt/processed/20240131/player_other_positions.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

--02. Проверяем доступ\контакт\связь в S3
select * from public.player_other_positions_parquet;

--03. Creаting stg table\Создаем таблицу для загрузки данных из S3


drop table stg.stg_player_other_positions CASCADE;
create table stg.stg_player_other_positions
(
	id text,
	position_other text,
	load_date date default  now()
);



--select * from public.player_other_positions_parquet;
--04. Проверяем создание таблицы c колонками

select * from stg.stg_player_other_positions;

--05.Загружаем данные по клубам из S3 через внешнюю таблицу в GP
 
TRUNCATE TABLE stg.stg_player_other_positions;

INSERT INTO stg.stg_player_other_positions
	select
		id,
	    position_other 
	FROM public.player_other_positions_parquet;

--06.Проверяем загрузку данных
select * from stg.stg_player_other_positions;

--6.PLAYER-OTHER-CITIZENSHIPS

--01. Creating external table\ создаем внешнюю таблицу player_other_citizenship в схеме public

DROP EXTERNAL TABLE IF EXISTS public.player_other_citizenship_parquet;
CREATE EXTERNAL TABLE public.player_other_citizenship_parquet (
	id text,
	citizenship text
)
LOCATION (
	'pxf://transfermarkt/processed/20240131/player_other_citizenship.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

--02. Проверяем доступ\контакт\связь в S3
select * from public.player_other_citizenship_parquet;

--03. Creаting stg table\Создаем таблицу для загрузки данных из S3


drop table stg.stg_player_other_citizenship CASCADE;
create table stg.stg_player_other_citizenship
(
	id text,
	citizenship text,
	load_date date default  now()
);



--select * from public.player_other_citizenship_parquet;
--04. Проверяем создание таблицы c колонками

select * from stg.stg_player_other_citizenship;

--05.Загружаем данные по клубам из S3 через внешнюю таблицу в GP
 
TRUNCATE TABLE stg.stg_player_other_citizenship;

INSERT INTO stg.stg_player_other_citizenship
	select
		id,
	    citizenship
	FROM public.player_other_citizenship_parquet;

--06.Проверяем загрузку данных
select * from stg.stg_player_other_citizenship;

--7.PLAYER-MARKET-VALUES

--01. Creating external table\ создаем внешнюю таблицу player_market_value в схеме public

DROP EXTERNAL TABLE IF EXISTS public.player_market_value_parquet;
CREATE EXTERNAL TABLE public.player_market_value_parquet (
	player_id text,
	market_date text,
	market_value text
)
LOCATION (
	'pxf://transfermarkt/processed/20240131/player_market_value.parquet?PROFILE=s3:parquet&SERVER=default'
) ON ALL
FORMAT 'CUSTOM' ( FORMATTER='pxfwritable_import' )
ENCODING 'UTF8';

--02. Проверяем доступ\контакт\связь в S3
select * from public.player_market_value_parquet;

--03. Creаting stg table\Создаем таблицу для загрузки данных из S3


drop table stg.stg_player_market_values CASCADE;
create table stg.stg_player_market_values
(
	player_id text,
	market_date text,
	market_value text,
	load_date date default  now()
);



--select * from public.player_market_value_parquet;
--04. Проверяем создание таблицы c колонками

select * from stg.stg_player_market_values;

--05.Загружаем данные по клубам из S3 через внешнюю таблицу в GP
 
TRUNCATE TABLE stg.stg_player_market_values;

INSERT INTO stg.stg_player_market_values
	select
		player_id,
	    market_date,
	    market_value
	FROM public.player_market_value_parquet;

--06.Проверяем загрузку данных
select * from stg.stg_player_market_values;

--END

--DBT
--STAGE

select * from stage.v_stg_clubs;
 
select * from stage.v_stg_player_market_values;

select * from stage.v_stg_player_citizenships;

select * from stage.v_stg_player_other_positions;

select * from stage.v_stg_club_players;

select * from stage.v_stg_club_profiles;

select * from stage.v_stg_player_profiles;

select * from stage.v_stg_player_positions;

--DV_STAGE

select * from dv_stage.v_dv_stage_clubs;

select * from dv_stage.v_dv_stage_club_profiles;

select * from dv_stage.v_dv_stage_player_profiles;
select * from dv_stage.v_dv_stage_player_profiles where id == 95810;

select * from dv_stage.v_dv_stage_positions;

select * from dv_stage.v_dv_stage_player_market_values;

select * from dv_stage.v_dv_stage_club_players;

select * from dv_stage.v_dv_stage_citizenships;

--DV
--hub

select * from dv.competitions_hub; 

select * from dv.clubs_hub;

select * from dv.seasons_hub;

select * from dv.citizenships_hub;

select * from dv.positions_hub;

select * from dv.position_types_hub;

select * from dv.players_hub;

--esat

select * from dv.club_competition_season_esat;

--link

select * from dv.player_citizenship_lnk;

select * from dv.player_position_type_lnk;

--sat

select * from dv.clubs_sat;

select * from dv.player_market_values_sat;

