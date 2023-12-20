#merge two dataframes using pyspark
from pyspark.sql.functions import *
simpleData=[(1,"sagar","CSE","UP",70),(2,"shivam","IT","TN",80),(3,"Raj","CIVIL","AP",50)]
columns=["ID","Student_Name","Department_Name","City","Score"]
dd=spark.createDataFrame(simpleData,columns)
dd.show()
simpleData_1=[(8,"mohan","CSE","UP"),(9,"Tarun","CIVIL","AP")]
columns_1=["ID","Student_Name","Department_Name","City"]
cc=spark.createDataFrame(simpleData_1,columns_1)
cc.show()
cc=cc.withColumn("Score",lit("null"))
ff=dd.union(cc)
ff.show()