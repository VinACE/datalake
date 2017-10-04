
val sqlContext =new org.apache.spark.sql.SQLContext(sc)



# val dfPerson=sqlContext.read.parquet("Datalake/< / hadoop folder >")
val df_schema = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option(na.string = "abc").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")


val df_schema = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")

df_schema.schema
val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")
df.select().write.mode(SaveMode.Append).format("orc").save("Datalake/glp_test2")


df.write.partitionBy("activ").mode(SaveMode.Append).format("parquet").save("Datalake/glp_test2")
df.write.partitionBy("activ").format("parquet").save("Datalake/glp_test2")
val df_glp = sqlContext.read.parquet("Datalake/glp_test1")

