import os
import configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct, current_timestamp, unix_timestamp, current_date
from pyspark.sql.types import StructType, StructField, StringType, LongType

def create_spark_session():
    # необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
    spark_jars_packages = ",".join(
            [
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0",
                "org.postgresql:postgresql:42.4.0",
            ]
        )
    
    # создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
    spark = SparkSession.builder \
                        .appName("RestaurantSubscribeStreamingService") \
                        .config("spark.sql.session.timeZone", "UTC") \
                        .config("spark.jars.packages", spark_jars_packages) \
                        .getOrCreate()
    return spark

def read_kafka_stream(spark_con, url, jaas):
    # читаем из топика Kafka сообщения с акциями от ресторанов 
    restaurant_read_stream_df = spark_con.readStream \
                                    .format('kafka') \
                                    .option('kafka.bootstrap.servers', url) \
                                    .option('kafka.security.protocol', 'SASL_SSL') \
                                    .option('kafka.sasl.jaas.config', jaas) \
                                    .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
                                    .option('subscribe', 'base') \
                                    .load()
    return restaurant_read_stream_df

def filter_stream_data(df):
    # Фильтрация данных из Kafka-стрима
    df = df.withColumn("value_string", df["value"].cast("string"))

    # определяем схему входного сообщения для json
    schema = StructType([
                            StructField("restaurant_id", StringType()),
                            StructField("adv_campaign_id", StringType()),
                            StructField("adv_campaign_content", StringType()),
                            StructField("adv_campaign_owner", StringType()),
                            StructField("adv_campaign_owner_contact", StringType()),
                            StructField("adv_campaign_datetime_start", LongType()),
                            StructField("adv_campaign_datetime_end", LongType()),
                            StructField("datetime_created", LongType())
                        ])

    # десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
    filtered_df = df.select(from_json(df["value_string"], schema).alias("json_data")).select("json_data.*")
    filtered_df = filtered_df.filter((col("adv_campaign_datetime_start") <= unix_timestamp(current_timestamp()).cast("long")) & (col("adv_campaign_datetime_end") >= unix_timestamp(current_timestamp()).cast("long")))
    return filtered_df

def read_subscribers_data(spark_con, url, user, password, driver):
    # вычитываем всех пользователей с подпиской на рестораны
    subscribers_restaurant_df = spark_con.read \
                            .format("jdbc") \
                            .option("url", url) \
                            .option("dbtable", "public.subscribers_restaurants") \
                            .option("user", user) \
                            .option("password", password) \
                            .option("driver", driver) \
                            .load()
    return subscribers_restaurant_df

def join_and_transform_data(filtered_data, subscribers_data):
    # джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
    result_df = filtered_data.join(subscribers_data, "restaurant_id")
    result_df = result_df.withColumn("current_date", current_date())
    return result_df

def save_to_postgresql_and_kafka(df, url_p, user_p, password_p, driver_p, url_k, jaas):
    df.writeStream.foreachBatch(write_to_postgresql(df, url_p, user_p, password_p, driver_p)).start().awaitTermination()
    df.writeStream.foreachBatch(write_to_kafka(df, url_k, jaas)).start().awaitTermination()

def write_to_postgresql(df, url, user, password, driver):
    try:
        df.write.format("jdbc") \
            .option("url", url) \
            .option("dbtable", "public.subscribers_feedback") \
            .option("user", user) \
            .option("password", password) \
            .option("driver", driver) \
            .mode("append") \
            .save()
    except Exception as e:
        print(f"Error writing to PostgreSQL: {str(e)}")

def write_to_kafka(df, url, jaas):
    try:
        persisted_df = df.persist()
        kafka_df = df.selectExpr("to_json(struct(*)) AS value")
        kafka_df.write.format("kafka") \
            .option('kafka.bootstrap.servers', url) \
            .option('kafka.security.protocol', 'SASL_SSL') \
            .option('kafka.sasl.jaas.config', jaas) \
            .option('kafka.sasl.mechanism', 'SCRAM-SHA-512') \
            .option("topic", "result_topic") \
            .option("value.serializer", "org.apache.kafka.common.serialization.StringSerializer") \
            .mode("append") \
            .save()
        persisted_df.unpersist()
    except Exception as e:
        print(f"Error writing to Kafka: {str(e)}")

def main():
    # Создаем объект конфигурации и читаем значения из файла
    config = configparser.ConfigParser()
    config.read('config.ini')

    # Получаем конфиги для postgres
    url_p = config.get('postgres_database', 'url')
    user_p = config.get('postgres_database', 'user')
    password_p = config.get('postgres_database', 'password')
    driver_p = config.get('postgres_database', 'driver')

    # Получаем конфиги для kafka
    url_k = config.get('kafka_conf', 'url')
    jaas_conf = config.get('kafka_conf', 'jaas_conf')

    spark = create_spark_session()
    restaurant_read_stream_df = read_kafka_stream(spark, url_k, jaas_conf)
    filtered_data = filter_stream_data(restaurant_read_stream_df)
    subscribers_data = read_subscribers_data(spark, url_p, user_p, password_p, driver_p)
    result_df = join_and_transform_data(filtered_data, subscribers_data)
    save_to_postgresql_and_kafka(result_df, url_p, user_p, password_p, driver_p, url_k, jaas_conf)
    spark.stop() 
    
