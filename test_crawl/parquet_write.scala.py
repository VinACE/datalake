
val sqlContext =new org.apache.spark.sql.SQLContext(sc)



val dfPerson=sqlContext.read.parquet("Datalake/< / hadoop folder>")


val df_schema = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")


val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")

