from pyspark.sql import SparkSession
import pyspark.sql.functions as f
df6=spark.read.format("csv").option("header","false").option("delimiter","|").load("/content/drive/MyDrive/Matches.csv")
headers=["Year","Wimbledon","Fr_Open","US_Open","Au_Open","end","po"]
df7=df6.toDF(*headers)
df7.show()
df8=spark.read.format("csv").option("header","true").option("delimiter",",").load("/content/drive/MyDrive/headertemplate.txt")
headers_1=df8.columns
df9=df6.toDF(*headers_1)
df9.show()