#read json file - Multi line & normal
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
spark=SparkSession.builder.appName("file format read").getOrCreate()
df14=spark.read.format("json").option("multiline", "true").load("/content/drive/MyDrive/USCities.json")
df14.show(truncate=False)
df15=spark.read.format("json").load("/content/drive/MyDrive/states.json")
df15.show(truncate=False)