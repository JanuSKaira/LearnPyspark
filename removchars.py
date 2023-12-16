#remove speacial chars and process multi delimiter file
from pyspark.sql import SparkSession
import pyspark.sql.functions as f
df10=spark.read.format("csv").option("header","true").option("delimiter","|").load("/content/drive/MyDrive/price.txt")
df11=df10.withColumn("Location",f.regexp_replace("Location",'[^A-Za-z0-9\s]',''))\
.withColumn("Status",f.regexp_replace("Status",'[^A-Za-z0-9\s]',''))
df11.show()
df12=spark.sparkContext.textFile("/content/drive/MyDrive/multi.csv")
df13=df12.map(lambda x: re.split('[#|]+',x)).toDF()
df13.show()