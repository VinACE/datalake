scala> case class Person(name:String,age:Int,sex:String)
defined class Person

scala>  val data=Seq(Person("Jack",25,"M"), Person("Jill",25,"F"),Person("abc",24,"F"))
data: Seq[Person] = List(Person(Jack,25,M), Person(Jill,25,F), Person(abc,24,F))

scala>  val data=Seq(Person("Jack",25,"M"), Person("Jill",25,"F"),Person("abc",24,"F"))
data: Seq[Person] = List(Person(Jack,25,M), Person(Jill,25,F), Person(abc,24,F))

scala> val df=data.toDF()
df: org.apache.spark.sql.DataFrame = [name: string, age: int ... 1 more field]

scala>

scala> import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SaveMode

scala>

scala> df.select("name","age","sex").write.mode(SaveMode.Append).format("parquet").save("/rmp/person")

scala> df.select("name","age","sex").write.mode(SaveMode.Append).format("parquet").save("/tmp/person")

scala> df.select("name","age","sex").write.mode(SaveMode.Append).format("parquet").save("Datalake/person")

scala> sqlContext
<console>:25: error: not found: value sqlContext
       sqlContext
       ^

scala>

scala> print(sqlContext
     | )
<console>:25: error: not found: value sqlContext
       print(sqlContext
             ^

scala> print(sqlContext)
<console>:25: error: not found: value sqlContext
       print(sqlContext)
             ^

scala> sc
res6: org.apache.spark.SparkContext = org.apache.spark.SparkContext@7d4f7aab

scala> val sqlContext =new org.apache.Spark.sql.SQLContext(sc)
<console>:25: error: object Spark is not a member of package org.apache
       val sqlContext =new org.apache.Spark.sql.SQLContext(sc)
                                      ^

scala> val dfPerson=sqlContext.read.parquet("/tmp/person")
<console>:24: error: not found: value sqlContext
       val dfPerson=sqlContext.read.parquet("/tmp/person")
                    ^

scala> val dfPerson=sc.read.parquet("Datalake/person")
<console>:25: error: value read is not a member of org.apache.spark.SparkContext
       val dfPerson=sc.read.parquet("Datalake/person")
                       ^

scala> import org.apache.spark.sql
import org.apache.spark.sql

scala> val sqlContext =new org.apache.Spark.sql.SQLContext(sc)
<console>:26: error: object Spark is not a member of package org.apache
       val sqlContext =new org.apache.Spark.sql.SQLContext(sc)
                                      ^

scala> import org.apache.Spark.sql
<console>:25: error: object Spark is not a member of package org.apache
       import org.apache.Spark.sql
                         ^

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

scala> dfPerson.show()
+----+---+---+
|name|age|sex|
+----+---+---+
|Jill| 25|  F|
| abc| 24|  F|
|Jack| 25|  M|
+----+---+---+


# https://medium.com/@mrpowers/working-with-s3-and-spark-locally-1374bb0a354

val df = sqlContext.read
.format("com.databricks.spark.csv")
.option("header", "true")
.option("inferSchema", "true")
.load("s3n://some_bucket/data/states/*.csv")

