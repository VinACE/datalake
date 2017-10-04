

val df_schema = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("charset", "UTF-8").load("s3n://fragrancedatalake/FragranceAugust2017/.csv")
val newNames = Seq("activ", "cmpno", "pltno", "glac1","glac2","glac3","glac4","gdesc","gtype")


val df_schema = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").option("inferSchema", "true").option("charset", "UTF-8").load("s3n://fragrancedatalake/FragranceAugust2017/glglp100.csv")