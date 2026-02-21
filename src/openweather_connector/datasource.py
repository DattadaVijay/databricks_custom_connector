# Databricks notebook source
# DBTITLE 1,Untitled
from pyspark.sql.datasource import DataSource, DataSourceReader, InputPartition
from pyspark.sql.types import StructType
import math
from pyspark.sql.types import *
import requests
import time

# COMMAND ----------

# Inorder to create a custom connector we need 3 classes.

# Class 1  - InputPartition (This is more to define the logical partioner)
# Class 2 - DataSource (Defines the name of the connector, schema and streamReader)
# Class 3 - DataSourceReader (This is where we define the logic that the executors should impliment)

# COMMAND ----------

# class 1 explicit on what key I want to partition

class OpenWeatherPartition(InputPartition):
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
            city STRING,
            date STRING,
            temp DOUBLE,
            humidity INT,
            description STRING
        """

    def reader(self, schema: StructType):
        return OpenWeatherReader(schema, self.options)

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

        if not self.api_key:
            raise Exception("api_key required")

    def partitions(self):
        return [OpenWeatherPartition(city.strip()) for city in self.cities if city.strip()]

    def get_lat_lon(self, city):
        geo_url = f"http://api.openweathermap.org/geo/1.0/direct?q={city}&limit=1&appid={self.api_key}"
        response = requests.get(geo_url)
        response.raise_for_status()
        geo_data = response.json()

        if not geo_data:
            raise Exception(f"City not found: {city}")

        return geo_data[0]["lat"], geo_data[0]["lon"]

    def read(self, partition: OpenWeatherPartition):
        city = partition.city
        lat, lon = self.get_lat_lon(city)

        for days_back in range(1, 11):  # last 10 days
            dt = datetime.utcnow() - timedelta(days=days_back)
            timestamp = int(dt.timestamp())

            url = (
                f"https://api.openweathermap.org/data/3.0/onecall/timemachine"
                f"?lat={lat}&lon={lon}&dt={timestamp}"
                f"&appid={self.api_key}&units=metric"
            )

            response = requests.get(url)
            response.raise_for_status()
            data = response.json()

            # Timemachine returns hourly data → take daily average
            temps = [hour["temp"] for hour in data.get("hourly", [])]
            humidity = [hour["humidity"] for hour in data.get("hourly", [])]
            description = data.get("hourly", [])[0]["weather"][0]["description"]

            if temps:
                avg_temp = sum(temps) / len(temps)
                avg_humidity = int(sum(humidity) / len(humidity))
            else:
                avg_temp = None
                avg_humidity = None

            yield (
                city,
                dt.strftime("%Y-%m-%d"),
                avg_temp,
                avg_humidity,
                description
            )


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