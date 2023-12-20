#explode using pyspark
from pyspark.sql.functions import col,explode
simpleData=[(1,"sagar",["UP","AP"]),(2,"shivam",["TN","PB"]),(3,"Raj",["AP","MP"])]
columns=["ID","Student_Name","City"]
ex_df=spark.createDataFrame(simpleData,columns)
ex_df.show()
exp_df=ex_df.select(col("ID"),col("Student_Name"),explode(col("City")))
exp_df.show()