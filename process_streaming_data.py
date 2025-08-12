from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, LongType

event_hub_connection_str = "Endpoint=sb://streamingdatanid.servicebus.windows.net/;SharedAccessKeyName=NideeshSharedAccessKey;SharedAccessKey=lY8xy5ZabcdEFGHijklmnOpQRStUVWxYZaBcDeFgHI=;EntityPath=lastfm-stream"

spark = SparkSession.builder.appName("LastFM_EventHub_Processor").getOrCreate()

ehConf = {
    'eventhubs.connectionString' : spark._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(event_hub_connection_str)
}

schema = StructType([
    StructField("user", StringType(), True),
    StructField("artist", StringType(), True),
    StructField("track", StringType(), True),
    StructField("album", StringType(), True),
    StructField("timestamp", LongType(), True),
    StructField("datetime", StringType(), True)
])

raw_df = (
    spark.readStream
    .format("eventhubs")
    .options(**ehConf)
    .load()
)

json_df = raw_df.select(from_json(col("body").cast("string"), schema).alias("data")).select("data.*")

clean_df = json_df.withColumn("artist", trim(lower(col("artist")))) \
  .withColumn("track", trim(lower(col("track")))) \
  .withColumn("album", trim(lower(col("album")))) \
  .withColumn("event_time", to_timestamp(col("datetime"), "dd MMM yyyy, HH:mm")) \
  .dropna(subset=["artist", "track", "event_time"])

def merge_to_delta(microBatchDF, batchId):
    microBatchDF.createOrReplaceTempView("updates")

    microBatchDF.sparkSession.sql("""
        MERGE INTO delta.`/mnt/delta/kpis/top_artists` AS target
        USING updates AS source
        ON target.artist = source.artist
        WHEN MATCHED AND source.plays_last_hour = 0 THEN
            DELETE
        WHEN MATCHED THEN
            UPDATE SET
                target.plays_last_hour = source.plays_last_hour,
                target.last_play_time = source.last_play_time
        WHEN NOT MATCHED THEN
            INSERT (artist, plays_last_hour, last_play_time)
            VALUES (source.artist, source.plays_last_hour, source.last_play_time)
    """)

# 1. Top 10 Artists in last 1 hour (sliding window)
df_with_ts = clean_df.withColumn(
    "event_time", from_unixtime("timestamp").cast("timestamp")
)
df_with_ts = df_with_ts.withWatermark("event_time", "1 hour")
window_spec = (
    Window
    .partitionBy(window("event_time", "1 hour", "5 minutes"), "artist") \
    .orderBy(col("event_time").cast("long")) \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow) \
)
top_artists = df_with_ts
  .withColumn("plays_last_hour", count("track").over(window_spec)) \
  .groupBy("artist", "plays_last_hour") \
  .agg(max("event_time").alias("last_play_time")) \
  .orderBy(desc("plays_last_hour")) \
  .limit(10) \

# 2. Total unique tracks played
unique_tracks = clean_df.agg(countDistinct("track").alias("unique_tracks"))

# 3. Most active users
active_users = clean_df.groupBy("user").count().orderBy(col("count").desc())

# Write KPI results to Delta tables
query1 = top_artists.writeStream \
    .foreachBatch(merge_to_delta) \
    .outputMode("update") \
    .option("checkpointLocation", "/mnt/delta/checkpoints/top_artists") \
    .trigger(processingTime="5 minutes") \
    .start()

query2 = unique_tracks.writeStream.outputMode("complete").format("delta") \
  .option("checkpointLocation", "/mnt/delta/checkpoints/unique_tracks") \
  .option("path", "/mnt/delta/kpis/unique_tracks") \
  .start()

query3 = active_users.writeStream.outputMode("complete").format("delta") \
  .option("checkpointLocation", "/mnt/delta/checkpoints/active_users") \
  .option("path", "/mnt/delta/kpis/active_users") \
  .start()

spark.streams.awaitAnyTermination()
