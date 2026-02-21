# Databricks notebook source
from pyspark.sql.datasource import InputPartition
from pyspark.sql.datasource import DataSource, DataSourceStreamReader
from typing import Iterator, Tuple
from faker import Faker
# COMMAND ----------
class FakePartition(InputPartition):
    def __init__(self, partition_id, num_rows_per_batch):
        self.partition_id = partition_id
        self.num_rows_per_batch = num_rows_per_batch
# COMMAND ----------
class FakeStreamDataSource(DataSource):
    @classmethod
    def name(cls):
        return "fakestream_partitioned"

    def schema(self):
        return "name STRING, email STRING, city STRING, partition_id INT"

    def streamReader(self, schema):
        return FakeStreamReader(schema, self.options)
# COMMAND ----------
class FakeStreamReader(DataSourceStreamReader):
    def __init__(self, schema, options):
        self.current_offset = 0
        self.faker = Faker()
        self.num_partitions = int(options.get("numPartitions", 3))
        self.num_rows_per_partition = int(options.get("numRowsPerPartition", 2))

    def initialOffset(self):
        """Start reading from offset 0"""
        return {"offset": 0}

    def latestOffset(self):
        """Increment offset for each microbatch"""
        self.current_offset += 1
        return {"offset": self.current_offset}

    def partitions(self, start, end):
        """
        Split the microbatch into partitions.
        Each partition generates `num_rows_per_partition` rows
        """
        return [
            FakePartition(pid, self.num_rows_per_partition)
            for pid in range(self.num_partitions)
        ]

    def read(self, partition: FakePartition) -> Iterator[Tuple]:
        """
        Generate fake rows for this partition.
        Each partition produces `num_rows_per_partition` rows per microbatch.
        """
        for _ in range(partition.num_rows_per_batch):
            yield (
                self.faker.name(),
                self.faker.email(),
                self.faker.city(),
                partition.partition_id
            )

    def commit(self, end):
        """
        Optional: called after Spark finishes this microbatch.
        We could log or clean up here.
        """
        print(f"Committed microbatch at offset {end['offset']}")

# COMMAND ----------
spark.dataSource.register(FakeStreamDataSource)

query = (
    spark.readStream
        .format("fakestream_partitioned")
        .option("numPartitions", 4)
        .option("numRowsPerPartition", 3)
        .load()
        .writeStream
        .format("console")
        .outputMode("append")
        .start()
)

query.awaitTermination()