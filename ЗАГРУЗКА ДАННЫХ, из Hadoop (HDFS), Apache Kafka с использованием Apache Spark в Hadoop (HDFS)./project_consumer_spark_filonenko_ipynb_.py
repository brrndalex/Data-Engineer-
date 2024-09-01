
"""Ppoject_consumer_Spark_Filonenko.ipynb"
"""

import findspark
findspark.init('/opt/spark')

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark = SparkSession.builder \
    .appName("PySparkProject") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1") \
    .getOrCreate()

kafka_bootstrap_servers = "kafka1:19091"
kafka_topic = "project_kafka"

# подготовить схему для датафрейма movies_df
movies_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("year", IntegerType()),
    StructField("rank", DoubleType())
])

# считываем поток из kafka
kafka_stream_df = (
    spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
    .option("subscribe", "project_kafka")
    .load()
)

# парсим файл movies в датафрейм с учетом его схемы
movies_df = (
    kafka_stream_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json("value", movies_schema).alias("data"))
    .select("data.*")
)


roles_df = spark.read.csv('/user/datasets/roles.csv', header = True, InferSchema= True) # считываем датафрейм из csv файла, загруженного в hdfs
actors_df = spark.read.csv('/user/datasets/actors.csv', header = True, InferSchema= True) # считываем датафрейм из csv файла, загруженного в hdfs
movies_directors_df = spark.read.csv('/user/datasets/movies_directors.csv', header = True, InferSchema= True) # считываем датафрейм из csv файла, загруженного в hdfs
directors_df = spark.read.csv('/user/datasets/directors.csv', header = True, InferSchema= True) # считываем датафрейм из csv файла, загруженного в hdfs
movies_genres_df = spark.read.csv('/user/datasets/movies_genres.csv', header = True, InferSchema= True) # считываем датафрейм из csv файла, загруженного в hdfs
directors_genres_df = spark.read.csv('/user/datasets/directors_genres.csv', header = True, InferSchema= True) # считываем датафрейм из csv файла, загруженного в hdfs

# вставляем сюда запрос на формирование витрины, переписанный на PySpark Dataframe API. (на основе sql запроса)
# result_df = (
#     movies_df
#     .select(
#         col("m.id").alias("movie_id"),
#         col("m.name").alias("movie_name"),
#         col("m.year").alias("movie_year"),
#         col("m.rank").alias("movie_rank"),
#         col("a.id").alias("actor_id"),
#         col("a.first_name").alias("actor_first_name"),
#         col("a.last_name").alias("actor_last_name"),
#         col("r.role").alias("actor_role"),
#         col("d.id").alias("director_id"),
#         col("d.first_name").alias("director_first_name"),
#         col("d.last_name").alias("director_last_name"),
#         col("g.genre").alias("movie_genre")
# )
#  .join(roles, on=movies.id == roles.movie_id, how="inner")
#  .join(actors, on=roles.actor_id == actors.id, how="inner")
#  .join(movies_directors, on=movies.id == movies_directors.movie_id, how="inner")
#  .join(directors, on=movies_directors.director_id == directors.id, how="inner")
#  .join(movies_genres, on=movies.id == movies_genres.movie_id, how="inner")
#  .join(directors_genres, on=(directors.id == directors_genres.director_id) & (movies_genres.genre == directors_genres.genre), how="inner")
# )

# вставляем сюда запрос на формирование витрины, переписанный на PySpark Dataframe API. (на основе sql запроса)
#result_df = (
#     movies_df
#     .select(
#         col("id").alias("movie_id"),
#         col("name").alias("movie_name"),
#         col("year").alias("movie_year"),
#         col("rank").alias("movie_rank"),
# )
#  .join(roles_df.select(col('movie_id')), movies_df.id == roles_df.movie_id, how="inner")
#  .join(actors_df.select(col('id')), roles_df.actor_id == actors_df.id, how="inner")
#  .join(movies_directors_df.select(col('movie_id')), movies_df.id == movies_directors_df.movie_id, how="inner")
#  .join(directors_df.select(col('id')), movies_directors_df.director_id == directors_df.id, how="inner")
#  .join(movies_genres_df.select(col('movie_id')), movies_df.id == movies_genres_df.movie_id, how="inner")
#  .join(directors_genres_df.select(col('movie_id')), directors_df.id == directors_genres_df.director_id, movies_genres_df.genre == directors_genres_df.genre, how="inner")
# )

# вставляем сюда запрос на формирование витрины, переписанный на PySpark Dataframe API. (на основе sql запроса)
result_df = (
    movies_df.alias("m")
    .join(roles_df.alias("r"), col("m.id") == col("r.movie_id"), how='inner')
    .join(actors_df.alias("a"), col("r.actor_id") == col("a.id"), how='inner')
    .join(movies_directors_df.alias("md"), col("m.id") == col("md.movie_id"), how='inner')
    .join(directors_df.alias("d"), col("md.director_id") == col("d.id"), how='inner')
    .join(movies_genres_df.alias("mg"), col("m.id") == col("mg.movie_id"), how='inner')
    .join(
        directors_genres_df.alias("dg"),
        (col("d.id") == col("dg.director_id")) & (col("mg.genre") == col("dg.genre")),
        how='inner'
    )
    .select(
        col("m.id").alias("movie_id"),
        col("m.name").alias("movie_name"),
        col("m.year").alias("movie_year"),
        col("m.rank").alias("movie_rank"),
        col("a.id").alias("actor_id"),
        col("a.first_name").alias("actor_first_name"),
        col("a.last_name").alias("actor_last_name"),
        col("r.role").alias("actor_role"),
        col("d.id").alias("director_id"),
        col("d.first_name").alias("director_first_name"),
        col("d.last_name").alias("director_last_name"),
        col("mg.genre").alias("movie_genre"),
    )
)


# Проверяем, что пайплайн возвращает результат
result_df.show(5)

# Записываем результат сборки витрины в формате parquet в hdfs директорию
query = (result_df
         .writeStream
         .outputMode("append")  # добавляем новые данные к старым
         .format("parquet")  # формат вывода
         .option("path", "/user/outputs")  # путь для сохранения готовой витрины
         .option("checkpointLocation", "/user/outputs/checkpoint")
         .trigger(processingTime='1 minutes')  # интервал обновления витрины
         .start())

# Запускаем пайплайн
query.awaitTermination()
