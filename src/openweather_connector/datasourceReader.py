# Databricks notebook source
%pip install faker
# COMMAND ----------
from pyspark.sql.datasource import InputPartition, DataSource, DataSourceStreamReader
from typing import Iterator, Tuple
from faker import Faker

# COMMAND ----------
# Partition by user_id
class UserPartition(InputPartition):
    def __init__(self, user_id):
        self.user_id = user_id

# COMMAND ----------
# Stream Reader
class UserStreamReader(DataSourceStreamReader):
    def __init__(self, schema, options):
        self.faker = Faker()
        self.user_ids = options.get("userIds", "user1,user2,user3").split(",")
        self.initial_load_done = False
        self.num_initial_rows = int(options.get("numInitialRows", 5))  # how many rows per user initially

    def initialOffset(self):
        return {"offset": 0}

    def latestOffset(self):
        # Increment offset just to trigger microbatch
        return {"offset": 1}

    def partitions(self, start, end):
        # Each user_id is a partition
        return [UserPartition(u) for u in self.user_ids]

    def read(self, partition: UserPartition) -> Iterator[Tuple]:
        # Initial load: generate N rows per user
        if not self.initial_load_done:
            for _ in range(self.num_initial_rows):
                yield (
                    partition.user_id,
                    self.faker.name(),
                    self.faker.city(),
                    self.faker.word()  # action
                )
        else:
            # Subsequent microbatches: 1 new row per user
            yield (
                partition.user_id,
                self.faker.name(),
                self.faker.city(),
                self.faker.word()  # action
            )

    def commit(self, end):
        # Mark initial load done after first batch
        self.initial_load_done = True

# COMMAND ----------
# DataSource class
class UserStreamDataSource(DataSource):
    @classmethod
    def name(cls):
        return "user_stream"

    def schema(self):
        return "user_id STRING, name STRING, city STRING, action STRING"

    def streamReader(self, schema):
        return UserStreamReader(schema, self.options)

# COMMAND ----------
# Register the connector
spark.dataSource.register(UserStreamDataSource)

# COMMAND ----------
# Use it in streaming query
query = (
    spark.readStream
         .format("user_stream")
         .option("userIds", "user1,user2,user3,user4")
         .option("numInitialRows", 5)
         .load()
         .writeStream
         .format("console")
         .trigger(once=True)
         .outputMode("append")
         .start()
)

query.awaitTermination()