# DBT схема
![](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./DBT%20%D1%81%D1%85%D0%B5%D0%BC%D0%B0.drawio.png)
-**Raw staging** - сырой,необработанный, неочищенный этап, уровень. Ссылается на те таблицы с данными, которые загружены из MinioS3, в формате parquet. Этот этап соответствует schema *stage*.  
-**DV staging** - промежуточный этап, уровень. Добавляются служебные поля. Происходит обогащение метаинформацией. Этот этап соответствует schema *dv_stage*.    
-**Data Vault** - завершающий этап. Происходит раскладывание данных по хабам, линкам и сателлитам. Этот этап соответствует schema *dv*.  
# Цель, задача DBT
Многофункциональный фреймворк для создания модели хранилища данных. Инструмент для работы с данными, которые в хранилище уже загружены, но с этими данными нужно провести ряд преобразований, чтобы их подготовть к использованию. Позволяет максимально быстро и удлбно описать необходимые трансформации и наполнить витрины данными. От DE требуется определить SELECT-запросами требуемую структуру данных.    
АutomateDV - пакет автоматизации DBT, который генерирует и выполняет ETL, необходимый для создания хранилища данных Data Vault2.0.  
# Действия
#### Установлен dbt-greenplum    
python -m pip install dbt-greenplum  
#### Инициализирован проект dbt
dbt init  
#### Созданы папки: 
- [dbt_project.yml](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./dbt_project.yml) -содержит всю информацию(конфигурацию) о проекте.
- [profiles.yml](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./profiles.yml) -настройка подключения к Greenplum.
#### Запуск(построение) проекта с учетом настроек(конфигурации) по проекту    
dbt build (весь проект)    
dbt build -s example (части с учетом конфигурации проекта)
#### Основные схемы для проекта в папке models 
- example(тестирование)
- stage
-  dv_stage
-  dv
#### Определены источники для построения моделей ( те данные, которые загружены в Greenplum, они являются основой для всех моделей) в следующей папке:  
- [profiles.yml](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./sources.yml)
#### Модели созданы в файле с расширением .sql  с использованием jinja-шаблонов(параметризованные SQL-запросы). В частности:
{{ config(alias='v_stg_club_players') }}  
select   
    cast(club_id as int)
    , cast(player_id as int)
    , load_date  
from {{ source('transfermarkt', 'stg_club_players') }}    
#### Проверял модели  
dbt compile    
#### Установлен и запущен АutomateDV  
automate-dv install  
dbt deps    
#### Созданы в файле с расширением .sql  с использованием jinja-шаблонов и запущены модели из схемы *dv_stage* и *dv*. Шаблоны на сайте. Команды на запуск моделей, в частности:  
 dbt run -s v_dv_stage_clubs     
 dbt run -s dv.hubs - сразу все хабы   
 dbt run -s clubs_hub - один конкретный хаб
#### 
