# **ЗАГРУЗКА ДАННЫХ из API в DWH.**
[![Typing SVG](https://readme-typing-svg.herokuapp.com?font=Fira+Code&pause=1000&color=4DF731&width=435&lines=%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90+%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5+;%D0%B8%D0%B7+API+%D0%B2+DWH.)](https://git.io/typing-svg)
# Цель,задача проекта
1. В "ручном" режиме: Получить "сырые" данные из API и сохранить их в MinioS3 в форматах json и parquet. Из MinioS3 загрузить данные в формате parquet в DWH, во временные таблицы GreenPlum. И в заключении из временных таблиц загрузить данные в спроектированные объекты Data Vault.
2. С помощью Аpache Airflow объединить все этапы в один общий, непрерывный поток, конвейер данных.

# Действия в рамках проекта
1 ЭТАП: Работал в VSCode. [Загрузил "сырые" данные](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./project_minio_transfermarkt.py) из REST API из https://transfermarkt-api.vercel.app с помощью Python (библиотека requests).  

2 ЭТАП: Запустил из командной строки [docker compose с Minio и с GreenPlum](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./compose.yml). Сохранил данные в Minio(в созданном объекте(бакет/корзина) для хранения) в виде json-файлов. [Для этого использовал Python, библиотеки boto3(библиотека для работы с AWS(S3)-Minio) и json](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./project_minio_transfermarkt.py).   
  
| Service |	Port |
|:--------|:-----|
|  Minio  | http://localhost:9001     |
|  GreenPlum       |  DBeaver host:localhost    |

3 ЭТАП: При помощи библиотеки pandas(pandas.json_normalize - метод преобразования вложенных json структур в плоский, табличный вид) [развернул json-файлы в табличные структуры, которые преобразовал в формат parquet](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./project_minio_transfermarkt.py), используя библиотеки pyarrow(извлекаем модуль parquet) и io (извлекаем класс BytesIO). Данные в формате parquet с помощью библиотеки boto3 [сохранил в Minio](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./project_minio_transfermarkt.py), в тот же бакет, в котором находятся ранее сохраненные json-файлы. 

4 ЭТАП: Работал в DBeaver, в GreenPlum. [Данные в формате parquet из Minio загрузил во временные таблицы GreenPlum, используя протокол pxf.](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./%D0%A4%D0%B8%D0%BB%D0%BE%D0%BD%D0%B5%D0%BD%D0%BA%D0%BE-SQL-%D0%B7%D0%B0%D0%BF%D1%80%D0%BE%D1%81%D1%8B(S3-GP%2C%20Dbt-DV).sql)  

5 ЭТАП: Произвел моделирование данных детального слоя DWH по типу Data Vault2.0, используя инструмент draw.io.  

6 ЭТАП: [В DWH из временных таблиц GreenPlum загрузил данные в спроектированные объекты по типу Data Vault2.0 с помощью DBT(и пакетом автоматизации automateDV).](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./%D0%A4%D0%B8%D0%BB%D0%BE%D0%BD%D0%B5%D0%BD%D0%BA%D0%BE-SQL-%D0%B7%D0%B0%D0%BF%D1%80%D0%BE%D1%81%D1%8B(S3-GP%2C%20Dbt-DV).sql) Работал в VSCode( [с DBT](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./dbt_project.yml)), и в DBeaver.  

7 ЭТАП: С помощью Аpache Airflow объединил этапы в один общий, непрерывный поток, конвейер данных. Работал в VSCode. [Запустил из командной строки docker compose с Minio, с GreenPlum, c Apache Airflow, c Postgres](https://github.com/brrndalex/Data-Engineer-Projects/blob/main/%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90%20%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%20%D0%B8%D0%B7%20API%20%D0%B2%20DWH./docker-compose.yaml). Используя Python, написал DAG для выполнения задач во всем конвейере. Используя веб-интерфейс Airflow через connector подключил Airflow с Minio, с GreenPlum. Работал в DBeaver, в GreenPlum. Создал функции, которые наполняют временные таблицы из Minio. Работал в веб-интерфейсе Airflow. Запустил DAG, запустил конвейер, отслеживал его работу.  
 
| Service |	Port |
|:--------|:-----|
|  Minio  | http://localhost:9001     |
|  GreenPlum       |  DBeaver host:localhost    |
|  Airflow       |  http://localhost:8080    |  

# Выводы по проекту
 УДАЛОСЬ: 1. В "ручном" режиме все получилось, а именно: Получить "сырые" данные из API и сохранить их в MinioS3 в форматах json и parquet. Из MinioS3 загрузить данные в формате parquet в DWH, во временные таблицы GreenPlum. А из временных таблиц загрузить данные в спроектированные объекты по типу Data Vault2.0. 2. Конвейер, получилось только следующее: С помощью Аpache Airflow объединить в один общий, непрерывный поток, конвейер данных, следующие этапы: Получить "сырые" данные из API и сохранить их в MinioS3 в форматах json и parquet. Из MinioS3 загрузить данные в формате parquet в DWH, во временные таблицы GreenPlum. НЕ УДАЛОСЬ: в DWH, из временных таблиц GreenPlum загрузить данные в спроектированные объекты по типу Data Vault2.0(запуск dbt-модели(astronomer cosmor))- с апреля 2024 закрыт доступ к API источника данных.
 
 # Используемые навыки и инструменты
 * Python (библиотеки: requests, boto3, json, pandas, pyarrow).
 * SQL.
 * Greenplum.
 * MinioS3.
 * DBT.
 * Data Vault 2.0.
 * Apache Airflow.
 * Командная строка Linux.
 * Docker compose.
 * VSCode.
 * DBeaver.
 * Draw.io.
  
# Статус
- [x] Завершен
