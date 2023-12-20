#skip line while loading data
df_skip=spark.read.csv("/content/drive/MyDrive/testskip.txt")
#df_skip.show()
rdd_skip=spark.sparkContext.textFile("/content/drive/MyDrive/testskip.txt").zipWithIndex().filter(lambda a:a[1]>3).map(lambda x:x[0].split(","))
rdd_skip_columns=rdd_skip.first()
main_rdd_skip=rdd_skip.filter(lambda a:a!=rdd_skip_columns)
m_df=main_rdd_skip.toDF(rdd_skip_columns)
m_df.show()