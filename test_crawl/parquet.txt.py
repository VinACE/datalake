from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import SQLContext

conf = SparkConf().setMaster("spark://10.20.43.249 :7077")
sc = SparkContext(conf=conf, appName="flightDataAnalysis")
sqlContext = SQLContext(sc)


# converts a line into tuple
def airlineTuple(line):
    values = line.split(",")

    return (
        values[0], values[1], values[2], values[3], values[4], values[5], values[6], values[7], values[8], values[9],
       )


# load the airline data and covert into an RDD of tuples
lines = sc.textFile("/mnt1/datalake/glglp100.csv").map(airlineTuple)

# convert the rdd into a dataframe
df = sqlContext.createDataFrame(lines, [ `activ` ,
  `cmpno` ,
  `pltno` ,
  `glac1` ,
  `glac2` ,
  `glac3` ,
  `glac4` ,
  `gdesc` ,
  `gtype`  ])

# save the dataframe as a parquet file in HDFS
df.write.parquet("hdfs://localhost:9000/user/bigdata/airline/input-parquet-spark")

data.write.parquet("user/hadoop/Datalake//parquet_12_9")