#mobile number regex using pyspark
from pyspark.sql.functions import col,explode
simpleData=[(1,"sagar",'U980654IO'),(2,"shivam",'9765431232'),(3,"Raj",'i98h65g555'),(3,"gaju",'789654321')]
columns=["ID","Student_Name","Phone"]
ph_df=spark.createDataFrame(simpleData,columns)
ph_df.show()
ep_df=ph_df.select("*").filter(col("Phone").rlike('^[0-9]*$'))
ep_df.show()