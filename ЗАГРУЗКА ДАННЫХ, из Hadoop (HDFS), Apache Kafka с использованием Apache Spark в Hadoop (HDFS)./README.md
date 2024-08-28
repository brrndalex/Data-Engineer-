# **ЗАГРУЗКА ДАННЫХ, из Hadoop (HDFS), Apache Kafka с использованием Apache Spark в Hadoop (HDFS).**
[![Typing SVG](https://readme-typing-svg.herokuapp.com?font=Fira+Code&pause=1000&color=4DF731&width=435&lines=%D0%97%D0%90%D0%93%D0%A0%D0%A3%D0%97%D0%9A%D0%90+%D0%94%D0%90%D0%9D%D0%9D%D0%AB%D0%A5%2C;%D0%B8%D0%B7+Hadoop+(HDFS)%2C+Apache+Kafka++;%D1%81+%D0%B8%D1%81%D0%BF%D0%BE%D0%BB%D1%8C%D0%B7%D0%BE%D0%B2%D0%B0%D0%BD%D0%B8%D0%B5%D0%BC+Apache+Spark;%D0%B2+Hadoop+(HDFS).)](https://git.io/typing-svg)  
# Цель,задача проекта
Подготовить данные в HDFS. Загрузить данные в топик Apache Kafka. Переписать запрос для сборки витрины с SQL на PySpark. Сформировать пайплайн, считывая Apache Spark-ом данные из HDFS и из Apache Kafka. Сохранить результат работы пайплайна каждую минуту в формате parquet в произвольную HDFS-директорию.

# Действия в рамках проекта
1 ЭТАП: Работал в VSCode. Из командной строки запустил Docker compose с Hadoop-кластером, состоящим из: Jupyter Notebook, Apache Kafka, Apache Spark, Hadoop, Hive.

2 ЭТАП: Работал в Jupyter Notebook. В HDFS загрузил csv-файлы: "directors_genres.csv", "movies_genres.csv", "actors.csv", "roles.csv", "directors.csv", "movies_directors.csv", "movies.csv".

3 ЭТАП: В Kafdrop(веб-интерфейс Apache Kafka) создал топик "project_movies".

4 ЭТАП: Работал в Jupyter Notebook. Код- библиотеки confluent_kafka(Producer), json, csv. Из файла "movies.csv" запустил загрузку записей в созданный топик Apache Kafka с условием: Каждую секунду в топик должна загружаться одна запись из файла. В Kafdrop в топике отслеживал как загружаются записи.

5 ЭТАП: Переписал запрос для сборки витрины с SQL на код PySpark.

6 ЭТАП: Работал в Jupyter Notebook. Сформировал пайплайн, в котором  с помощью Apache Spark считывал данные из Apache Kafka и из HDFS(csv-файлы). Код- библиотеки findspark, pyspark.sql, pyspark.sql.functions, pyspark.sql.types, запрос для сборки витрины на PySpark. Сохранил результат работы пайплайна каждую минуту в формате parquet в произвольную HDFS директорию. 

# Выводы по проекту
 Сформирован пайплайн, в результате которого считаны Apache Spark-ом данные из HDFS(csv-файлы) и из Apache Kafka(загружаемые в топик записи из csv-файла). Запрос для сборки витрины с SQL переписан на код PySpark. Результат работы пайплайна сохранен в формате parquet в HDFS-директории.
 
 # Используемые навыки и инструменты
 * Python (библиотеки: confluent_kafka(Producer), json, csv).
 * Apache Spark (PySpark).
 * Apache Hadoop (HDFS).
 * Jupyter Notebook.
 * Docker compose.
 
# Статус
- [x] Завершен
