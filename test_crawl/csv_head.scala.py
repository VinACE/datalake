val sqlContext = new org.apache.spark.sql.SQLContext(sc)

# val dfPerson = sqlContext.read.parquet("Datalake/< / hadoop folder>")

file_list = []

val df_schema = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")

# print the Schema
df_schema

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")


val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")

df.select().write.mode(SaveMode.Append).format("parquet").save("Datalake/ +   + < / hadoop folder>")


df.write.mode(SaveMode.Append).format("parquet").save("Datalake/glp_test1")
val df_glp=sc.read.parquet("Datalake/glp_test1")