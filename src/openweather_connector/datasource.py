# Databricks notebook source
# DBTITLE 1,Untitled
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType
import math
from pyspark.sql.types import *
import requests

# COMMAND ----------

# Inorder to create a custom connector we need 3 classes.

# Class 1  - InputPartition (This is more to define the logical partioner)
# Class 2 - DataSource (Defines the name of the connector, schema and streamReader)
# Class 3 - DataSourceReader (This is where we define the logic that the executors should impliment)

# COMMAND ----------

# class 1 explicit on what key I want to partition

class openWeatherPartiton(InputPartition):
    def __init__(self, city):
        self.city = city

# COMMAND ----------

# Class 2 - Here we are definign the attributes of connector itself, like name, schema and the class that executes the read

class OpenWeatherDataSource(DataSource):

    @classmethod
    def name(cls):
        return "openweather"

    def schema(self):
        return """
            city string,
            temperature double,
            humidity int,
            weather string
        """

    def reader(self, schema: StructType):
        return OpenWeatherDataSource(self.options)

# COMMAND ----------

# DBTITLE 1,Cell 5
# class 3 the actual executor itself. The class that brings partions and actual execution to life


class OpenWeatherDataSource(DataSource):
    @classmethod
    def name(cls):
        return "openweather"

    def schema(self):
        return "city STRING, temp DOUBLE, humidity INT, description STRING"

    def reader(self, schema: StructType):
        return OpenWeatherReader(schema, self.options)
    

class OpenWeatherReader(DataSourceReader):
    def __init__(self, schema, options):
        self.schema = schema
        self.api_key = options.get("api_key")
        self.cities = options.get("cities", "").split(",")

    def partitions(self):
        return [openWeatherPartiton(city) for city in self.cities]

    def read(self, partition: openWeatherPartiton):
        city = partition.city
        url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={self.api_key}&units=metric"
        
        response = requests.get(url)
        if response.status_code == 200:
            data = response.json()
            yield (
                city, 
                float(data['main']['temp']), 
                int(data['main']['humidity']), 
                data['weather'][0]['description'])


# COMMAND ----------

# MAGIC %md
# MAGIC ### So in simnple terms we have 3 main classes and standard definintions.
# MAGIC
# MAGIC Class 1 inherits InputPartions -> Simple what ever is in init will be the partition key.
# MAGIC Class 2 inherits DataSource class -> Very simple it has 3 methods -> 1. classmethod its name(), schema() and reader() -> reader takes input 
# MAGIC class 3 inherits DataSourceReader class -> it has 2 methods -> 1. partitions() and read() -> read needs partition: (the class for partitioning)

# COMMAND ----------

spark.dataSource.register(OpenWeatherDataSource)

# COMMAND ----------

df = spark.read.format("openweather").option("api_key", "4075c5aa39b8d50adc3f0a606160e7d2").option("cities", "London,Paris,Vancouver").load()

# COMMAND ----------

df.display()