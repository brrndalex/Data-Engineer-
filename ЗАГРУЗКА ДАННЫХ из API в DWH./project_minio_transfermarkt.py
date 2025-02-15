# -*- coding: utf-8 -*-
"""PROJECT_minio_transfermarkt.ipynb

Automatically generated by Colab.

Original file is located at
    https://colab.research.google.com/drive/1xpc7oudCw6NahZj3Xv3ANxQnWxe4CG8v

Данные  для работы с объектным хранилищем на основе Minio
"""

S3_ACCESS_KEY = "minio"
S3_SECRET_KEY = "minio123"
S3_TEST_BUCKET = "transfermarkt"
S3_ENDPOINT = "http://localhost:9000"
DT = "20240224"

"""1. CLUBS

API ресурса transfermarkt для клубов(CLUBS).
"""

clubs_api = "https://transfermarkt-api.vercel.app/competitions/gb1/clubs?season_id = 2023"

"""1. Импорт библиотеки requests(запросов)
2. Отправка запроса на получение списка клубов по API  в формате json.
3. Получение списка клубов.
"""

import requests as r

clubs = r.get(clubs_api).json()
clubs

"""     Сохранение списка клубов в создаваемом на основе Minio ресурсе
     
1. Импрот библиотек boto3 и json (boto3-библиотека для работы с AWS).
2. Создание интерфейса для работы с сервисом AWS(S3) с помощью библиотеки boto3.
3. Создание объекта для хранения с именем бакета/корзины "transfermarkt" (объект `obj` будет представлять файл "clubs.json" внутри корзины/бакета "transfermarkt", по заданному пути/ключу "raw-data/{DT}/clubs.json").
4.  Загрузка сериализованных(преобразованных) в строку и закодированных данных `clubs`(список клубов) в файл "clubs.json" внутри корзины/бакета "transfermarkt"(Body: тело данных, которые будут загружены в объект, при этом данные `clubs` будут сериализованы в формат JSON (с помощью `json.dumps()`) и затем закодированы (с помощью `encode()`) для передачи через сеть).
5. Смотрим в Minio в бакете "transfermarkt" по пути/ключу "raw-data/20231211/" файл clubs.json.

"""

import boto3
import json

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

obj = s3.Object(bucket_name = "transfermarkt", key = f"raw-data/{DT}/clubs.json")

obj.put(Body = json.dumps(clubs).encode())

"""Преобразование в "clubs_json" данных из файла "clubs.json" в вид Python объекта (обычно словаря или списка), готового для использования

1. Импрот библиотек boto3 и json (boto3-библиотека для работы с AWS).
2. Создание интерфейса для работы с сервисом AWS(S3) с помощью библиотеки boto3.
3. Создание объекта для хранения с именем бакета/корзины "transfermarkt" (объект `obj` будет представлять файл "clubs.json" внутри корзины/бакета "transfermarkt", по заданному пути/ключу "raw-data/{DT}/clubs.json").
4. Считывание содержимого файла "clubs.json" с помощью методов `get()` и `read()`.
5. Полученные данные декодируются из байтового формата (с использованием `decode()`) и десериализуются из формата JSON (с использованием `json.loads()`).
6. Сохранение результата  в переменную `clubs_json`, содержащую декодированный файл "clubs.json".

"""

import boto3
import json

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

obj = s3.Object(bucket_name = "transfermarkt", key = f"raw-data/{DT}/clubs.json")

clubs_json = json.loads(obj.get()["Body"].read().decode())
clubs_json

"""Преобразование данных в Датафрейм, в таблицу, в структурированный вид.
1. Импорт библиотеки pandas.
2. Применение pandas.json_normalize - метода преобразования вложенных json структур в плоский, табличный вид.
3. Здесь вместо pandas можно использовать Apache Spark.
"""

import pandas as pd

clubs_df = pd.json_normalize(clubs_json, meta = ["id", "name", "seasonID"], meta_prefix = "league_", record_path = "clubs", record_prefix ="clubs_")

clubs_df

"""Преобразование сырых данных  в формат parquet

1. Импорт библиотеки pyarrow. Импорт из библиотеки pyarrow модуля parquet.
2. Импорт из библиотеки io класс BytesIO(`BytesIO()` - это класс из модуля `io` в Python, который предоставляет удобный способ работы с данными в виде байтового буфера в памяти.BytesIO()` позволяет временно хранить и манипулировать данными в памяти перед их дальнейшей обработкой или записью.).
3. Создание таблицы `table` с использованием метода `Table.from_pandas()` модуля `pyarrow`. В качестве аргумента передается DataFrame `clubs_df`, который преобразуется в таблицу формата `pyarrow`.
4. Создание `buf` - это переменная, которая инициализируется как объект класса `BytesIO()`, то есть создается новый байтовый буфер в памяти.В данном конкретном коде, объект класса `BytesIO()` используется для временного хранения данных перед их записью в S3 или другой источник данных.
5. pq.write_table(table, buf) - запись таблицы `table` в буфер `buf` с использованием функции `write_table()` из модуля `pyarrow.parquet`. Процесс записи в формат Parquet включает сериализацию данных таблицы и сохранение их в буфере. Таким образом, `buf` после выполнения этой строки будет содержать данные таблицы `table` в формате Parquet в виде байтового объекта, готового к дальнейшей обработке или сохранению.
6. Создание ресурса `s3` с использованием метода `boto3.resource()`, передавая необходимые параметры для доступа к корзине/бакету, такие как `service_name`, `endpoint_url`, `aws_access_key_id` и `aws_secret_access_key`. И создание объект `obj` с использованием метода `Object()` ресурса `s3`. Происходит указание имени корзины/бакета (`bucket_name`) и пути/ключа файла `clubs.parquet` (`key`). При этом формат пути/ключа содержит переменную `{DT}`, которая будет заменена на соответствующее значение в момент выполнения кода.
7. Загрузка данных из буфера `buf` в объект `obj` с использованием метода `put()`. Данные получаются с помощью метода `getvalue()` из буфера `buf`.
"""

import pyarrow

from pyarrow import parquet as pq
from io import BytesIO

table = pyarrow.Table.from_pandas(clubs_df)
buf = BytesIO()

pq.write_table(table, buf)

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

obj = s3.Object(bucket_name = "transfermarkt", key = f"processed/{DT}/clubs.parquet")

obj.put(Body = buf.getvalue())

"""2. CLUB-PROFILES"""

clubs_ids = [i["id"] for i in clubs_json["clubs"]]

for i in clubs_ids:
    club_profile = r.get(f"https://transfermarkt-api.vercel.app/clubs/{i}/profile").json()
    obj = s3.Object(bucket_name = "transfermarkt", key = f"raw-data/{DT}/club-profiles/club_profile_{i}.json")

    obj.put(Body = json.dumps(club_profile).encode())

bucket = s3.Bucket(name = "transfermarkt")

club_profile_list = [obj.key for obj in bucket.objects.filter(Prefix = f"raw-data/{DT}/club-profile")]

club_profile_list

import boto3
import json
import pandas as pd

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

bucket = s3.Bucket(name = "transfermarkt")

club_profile_list = [obj.key for obj in bucket.objects.filter(Prefix = f"raw-data/{DT}/club-profile")]


data1 = []
for club_profile in club_profile_list:
    obj = s3.Object(bucket_name="transfermarkt", key=club_profile)
    clubs_json = json.loads(obj.get()["Body"].read().decode())
    data1.append(clubs_json)

club_profiles_df = pd.json_normalize(data1)[["id","name","officialName", "addressLine1","addressLine2", "addressLine3","stadiumName","stadiumSeats","currentMarketValue"]]

club_profiles_df

import pyarrow

from pyarrow import parquet as pq
from io import BytesIO

table = pyarrow.Table.from_pandas(club_profiles_df)
buf = BytesIO()

pq.write_table(table, buf)

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

obj = s3.Object(bucket_name = "transfermarkt", key = f"processed/{DT}/club_profiles.parquet")

obj.put(Body = buf.getvalue())

"""3. CLUB-PLAYERS"""

# Перечень URL club-profile

from string import Template

a=Template("https://transfermarkt-api.vercel.app/clubs/$club/players")

for id in clubs_ids:
    print(a.substitute(club = id))

import requests as r

clubs_api = "https://transfermarkt-api.vercel.app/competitions/gb1/clubs?season_id = 2023"
data = r.get(clubs_api).json()
data
#clubs_ids = [i["id"] for i in clubs_json["clubs"]]



# clubs_ids = []

# for i in clubs_json['clubs']:
# #     #clubs_ids.append(i)
#       clubs_ids.append(i['id'])

# clubs_ids

club_player = r.get(f"https://transfermarkt-api.vercel.app/clubs/281/players").json()
club_player

#https://transfermarkt-api.vercel.app/clubs/281/players

clubs_ids = []
for i in data["clubs"]:
    clubs_ids.append(i['id'])

clubs_ids

for i in clubs_ids:
    club_player = r.get(f"https://transfermarkt-api.vercel.app/clubs/{i}/players").json()
    obj = s3.Object(bucket_name = "transfermarkt", key = f"raw-data/{DT}/club-players/club_player_{i}.json")

    obj.put(Body = json.dumps(club_player).encode())

bucket = s3.Bucket(name = "transfermarkt")

club_player_list = [obj.key for obj in bucket.objects.filter(Prefix = f"raw-data/{DT}/club-player")]

club_player_list

import boto3
import json
import pandas as pd

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

bucket = s3.Bucket(name = "transfermarkt")

club_player_list = [obj.key for obj in bucket.objects.filter(Prefix = f"raw-data/{DT}/club-player")]

data_player = []
for club_player in club_player_list:
    obj = s3.Object(bucket_name="transfermarkt", key=club_player)
    clubs_player_json = json.loads(obj.get()["Body"].read().decode())
    data_player.append(clubs_player_json)

data_player

# club_players_df = pd.json_normalize(data_player)

# #club_players_df = pd.json_normalize(data_player, meta = 'id', meta_prefix = 'club_', record_path= 'players', record_prefix = 'player_')[['player_id', 'player_foot']]

club_players_df = pd.json_normalize(data_player, meta = 'id', meta_prefix = 'club_', record_path= 'players', record_prefix = 'player_')[['club_id', 'player_id']]

club_players_df

#Преобразование данных в parquet

import pyarrow

from pyarrow import parquet as pq
from io import BytesIO

table = pyarrow.Table.from_pandas(club_players_df)
buf = BytesIO()

pq.write_table(table, buf)

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

obj = s3.Object(bucket_name = "transfermarkt", key = f"processed/{DT}/club_players.parquet")

obj.put(Body = buf.getvalue())

"""4. PLAYER-PROFILES"""

for i in clubs_ids:
    club_player = r.get(f"https://transfermarkt-api.vercel.app/clubs/{i}/players").json()
    obj = s3.Object(bucket_name = "transfermarkt", key = f"raw-data/{DT}/club-players/club_player_{i}.json")

    obj.put(Body = json.dumps(club_player).encode())

import boto3
import json
import pandas as pd

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

bucket = s3.Bucket(name = "transfermarkt")

club_player_list = [obj.key for obj in bucket.objects.filter(Prefix = f"raw-data/{DT}/club-player")]

data_player = []
for club_player in club_player_list:
    obj = s3.Object(bucket_name="transfermarkt", key=club_player)
    clubs_player_json = json.loads(obj.get()["Body"].read().decode())
    data_player.append(clubs_player_json)

data_player

players = []        # создаем пустой список для сохранения профилей players

for club in data_player:               # проходим по каждому элементу списка data_player
    player = club.get('players') # получаем значение ключа 'players' для текущего элемента
    #players.append(player)
    players.extend(player)     # добавляем значения ключа 'players' к общему списку
for player in  players:
    print(player)

players

players_ids = []
for i in players:                         # проходим по каждому профилю игрока
    players_ids.append(i['id'])
    #clubs_ids.append(i)

print(f"Общее количество игроков: {len(players_ids)}")

players_ids  # список ID - ИГРОКОВ

# Перечень URL player-profiles

# https://transfermarkt-api.vercel.app/players/238223/profile

from string import Template

a=Template("https://transfermarkt-api.vercel.app/players/$player/profile")

for id in players_ids:
    print(a.substitute(player = id))

import time

for i in players_ids:
    player_profile = r.get(f"https://transfermarkt-api.vercel.app/players/{i}/profile").json()
    obj = s3.Object(bucket_name = "transfermarkt", key = f"raw-data/{DT}/player_profiles/player_profile_{i}.json")

    obj.put(Body = json.dumps(player_profile).encode())

    #time.sleep(3)

bucket = s3.Bucket(name ="transfermarkt")

player_profile_list = [obj.key for obj in bucket.objects.filter(Prefix = f"raw-data/{DT}/player_profiles")]

player_profile_list

#print(len(player_profile_list))

print(len(player_profile_list))

import boto3
import json
import pandas as pd

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

bucket = s3.Bucket(name = "transfermarkt")

player_profile_list = [obj.key for obj in bucket.objects.filter(Prefix = f"raw-data/{DT}/player_profiles")]

data_player_profile = []
for player_profile in player_profile_list:
    obj = s3.Object(bucket_name="transfermarkt", key=player_profile)
    player_profile_json = json.loads(obj.get()["Body"].read().decode())
    data_player_profile.append(player_profile_json)

data_player_profile

#club_players_df = pd.json_normalize(data_player_profile)

#club_players_df = pd.json_normalize(data_player_profile, meta = 'id', meta_prefix = 'club_', record_path= 'players', record_prefix = 'player_')[['player_id', 'player_foot']]

# "id" "name" "fullName" "dateOfBirth"

player_profiles_df = pd.json_normalize(data_player_profile, sep="_") [['id','name', 'fullName', 'dateOfBirth', 'height', 'isRetired', 'foot', 'placeOfBirth_country', 'position_main', 'club_joined', 'club_contractExpires', 'agent_name',	'description', 'nameInHomeCountry']]   #, meta = 'id', meta_prefix = 'club_', record_path= 'players', record_prefix = 'player_')[['club_id', 'player_id']]

player_profiles_df

#Преобразование данных в parquet

import pyarrow

from pyarrow import parquet as pq
from io import BytesIO

table = pyarrow.Table.from_pandas(player_profiles_df)
buf = BytesIO()

pq.write_table(table, buf)

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

obj = s3.Object(bucket_name = "transfermarkt", key = f"processed/{DT}/player_profiles.parquet")


obj.put(Body = buf.getvalue())

"""5. PLAYER-OTHER-POSITIONS"""

data_player_profile

player_other_positions_df = pd.json_normalize(data_player_profile, sep='_')[['id','position_other']]

#player_other_positions_df = pd.json_normalize(data_player_profile,  meta = 'id', meta_prefix = 'player_', record_path= ["position", ["other"]],  record_prefix = 'position_')#[['player_id','position_other']]
#(player_profiles, record_path=["position", ["other"]], meta="id",   meta_prefix="player_")
player_other_positions_df

player_other_positions_df = player_other_positions_df.rename(columns={'id':'player_id'})

player_other_positions_df

# Преобразование списка в строку

#player_other_positions_df['position_other'] = player_other_positions_df['position_other'].apply(', '.join)
#df['position_other'] = df['position_other'].apply(lambda x: ', '.join(x))
player_other_positions_df['position_other'] = player_other_positions_df['position_other'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

player_other_positions_df

""" Давайте разберем этот код более подробно:

1. player_other_positions_df['position_other'] - это столбец "position_other" в DataFrame player_other_positions_df, к которому мы применяем изменение.

2. apply() - это метод Pandas, который применяет функцию к каждому элементу столбца.

3. lambda x: - это анонимная функция. Здесь x представляет собой значение каждой ячейки в столбце "position_other".

4. if isinstance(x, list) - это условное выражение, которое проверяет, является ли значение x списком.

5. , join(x) - это метод строки, который объединяет элементы списка x с помощью запятых.

6. else x - это часть условного выражения, которая возвращает значение x, если оно не является списком (например, если значение уже является строкой).

Таким образом, код применяет функцию lambda к каждому значению в столбце "position_other". Если значение является списком, то оно преобразуется в строку, объединив элементы списка с помощью запятых. Если значение не является списком (например, уже является строкой), то значение остается неизменным.

Новое значение присваивается столбцу "position_other" в DataFrame, и в результате список ['Left-Back', 'Defensive Midfield'] становится строкой 'Left-Back, Defensive Midfield', как требуется.
"""

player_other_positions_df = player_other_positions_df.rename(columns={'position_other':'position'})

player_other_positions_df

#Преобразование данных в parquet

import pyarrow

from pyarrow import parquet as pq
from io import BytesIO

table = pyarrow.Table.from_pandas(player_other_positions_df)
buf = BytesIO()

pq.write_table(table, buf)

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

obj = s3.Object(bucket_name = "transfermarkt", key = f"processed/{DT}/player_other_positions.parquet")

obj.put(Body = buf.getvalue())

"""6. PLAYER-OTHER-CITIZENSHIPS"""

player_other_citizenship_df = pd.json_normalize(data_player_profile)[['id', 'citizenship']]   #, meta = 'id', meta_prefix = 'club_', record_path= 'players', record_prefix = 'player_')[['club_id', 'player_id']]

player_other_citizenship_df

# Преобразование списка в строку

player_other_citizenship_df['citizenship'] = player_other_citizenship_df['citizenship'].apply(lambda x: ', '.join(x) if isinstance(x, list) else x)

player_other_citizenship_df

#Преобразование данных в parquet

import pyarrow

from pyarrow import parquet as pq
from io import BytesIO

table = pyarrow.Table.from_pandas(player_other_citizenship_df)
buf = BytesIO()

pq.write_table(table, buf)

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

obj = s3.Object(bucket_name = "transfermarkt", key = f"processed/{DT}/player_other_citizenship.parquet")

obj.put(Body = buf.getvalue())

"""7. PLAYER-MARKET-VALUES"""

# Перечень URL player-profiles

# https://transfermarkt-api.vercel.app/players/238223/market_value

from string import Template

a=Template("https://transfermarkt-api.vercel.app/players/$player/market_value")

for id in players_ids:
    print(a.substitute(player = id))

import requests as r

players_api = "https://transfermarkt-api.vercel.app/players/238223/market_value"
data_mv = r.get(players_api).json()
data_mv

import time

for i in players_ids:
    player_market_value = r.get(f"https://transfermarkt-api.vercel.app/players/{i}/market_value").json()
    obj = s3.Object(bucket_name = "transfermarkt", key = f"raw-data/{DT}/player_market_values/player_market_value_{i}.json")

    obj.put(Body = json.dumps(player_market_value).encode())

    #time.sleep(3)

bucket = s3.Bucket(name = "transfermarkt")

player_market_value_list = [obj.key for obj in bucket.objects.filter(Prefix = f"raw-data/{DT}/player_market_value")]

player_market_value_list

print(len(player_market_value_list))

import boto3
import json
import pandas as pd

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

bucket = s3.Bucket(name = "transfermarkt")

player_profile_list = [obj.key for obj in bucket.objects.filter(Prefix = f"raw-data/{DT}/player_market_values")]

data_player_market_value = []
for player_market_value in player_market_value_list:
    obj = s3.Object(bucket_name="transfermarkt", key=player_market_value)
    player_market_value_json = json.loads(obj.get()["Body"].read().decode())
    data_player_market_value.append(player_market_value_json)

data_player_market_value

player_market_value_df = pd.json_normalize(data_player_market_value, meta = 'id', meta_prefix ='player_', record_path ='marketValueHistory', record_prefix = 'market_') [['player_id', 'market_date', 'market_value']]   #, meta = 'id', meta_prefix = 'club_', record_path= 'players', record_prefix = 'player_')[['club_id', 'player_id']]

player_market_value_df

#Преобразование данных в parquet

import pyarrow

from pyarrow import parquet as pq
from io import BytesIO

table = pyarrow.Table.from_pandas(player_market_value_df)
buf = BytesIO()

pq.write_table(table, buf)

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

obj = s3.Object(bucket_name = "transfermarkt", key = f"processed/{DT}/player_market_value.parquet")

obj.put(Body = buf.getvalue())

"""ПОЛУЧЕНИЕ ID-CLUBs"""

import requests as r

clubs_api = "https://transfermarkt-api.vercel.app/competitions/gb1/clubs?season_id = 2023"
data = r.get(clubs_api).json()
data

clubs_ids = []
for i in data["clubs"]:
    clubs_ids.append(i['id'])

clubs_ids

"""ПОЛУЧЕНИЕ ID-PLAYERs"""

for i in clubs_ids:
    club_player = r.get(f"https://transfermarkt-api.vercel.app/clubs/{i}/players").json()
    obj = s3.Object(bucket_name = "transfermarkt", key = f"raw-data/{DT}/club-players/club_player_{i}.json")

    obj.put(Body = json.dumps(club_player).encode())

import boto3
import json
import pandas as pd

s3 = boto3.resource(service_name = "s3", endpoint_url = S3_ENDPOINT, aws_access_key_id = S3_ACCESS_KEY, aws_secret_access_key = S3_SECRET_KEY)

bucket = s3.Bucket(name = "transfermarkt")

club_player_list = [obj.key for obj in bucket.objects.filter(Prefix = f"raw-data/{DT}/club-player")]

data_player = []
for club_player in club_player_list:
    obj = s3.Object(bucket_name="transfermarkt", key=club_player)
    clubs_player_json = json.loads(obj.get()["Body"].read().decode())
    data_player.append(clubs_player_json)

data_player

players = []        # создаем пустой список для сохранения профилей players

for club in data_player:               # проходим по каждому элементу списка data_player
    player = club.get('players') # получаем значение ключа 'players' для текущего элемента
    #players.append(player)
    players.extend(player)     # добавляем значения ключа 'players' к общему списку
for player in  players:
    print(player)

players

players_ids = []
for i in players:                         # проходим по каждому профилю игрока
    players_ids.append(i['id'])
    #clubs_ids.append(i)

print(f"Общее количество игроков: {len(players_ids)}")

players_ids  # список ID - ИГРОКОВ

# Перечень URL player-profiles

# https://transfermarkt-api.vercel.app/players/238223/profile

from string import Template

a=Template("https://transfermarkt-api.vercel.app/players/$player/profile")

for id in players_ids:
    print(a.substitute(player = id))