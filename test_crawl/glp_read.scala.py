
scala> val sqlContext =new org.apache.spark.sql.SQLContext(sc)
warning: there was one deprecation warning; re-run with -deprecation for details
sqlContext: org.apache.spark.sql.SQLContext = org.apache.spark.sql.SQLContext@434ad1f8

scala> val dfPerson=sc.read.parquet("Datalake/person")
<console>:26: error: value read is not a member of org.apache.spark.SparkContext
       val dfPerson=sc.read.parquet("Datalake/person")
                       ^

scala> val dfPerson=sqlContext.read.parquet("Datalake/person")
dfPerson: org.apache.spark.sql.DataFrame = [name: string, age: int ... 1 more field]

scala>

scala> val dfPerson=sqlContext.read.parquet("Datalake/person")
dfPerson: org.apache.spark.sql.DataFrame = [name: string, age: int ... 1 more field]

fragrancedatalake', 'FragranceAugust2017/glglp100.csv

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")


val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")

val df = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")
df.select().write.mode(SaveMode.Append).format("parquet").save("Datalake/glp_test")


df.write.mode(SaveMode.Append).format("parquet").save("Datalake/glp_test1")
val df_glp=sc.read.parquet("Datalake/glp_test1")