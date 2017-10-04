ll=["PRDSHPF","apapp100","apapp200","ararp100","ararp200","arccp200","glglp100","glsrp100","inpop100","inpop10h","inpop300","inpop30h","inpop400","kcdep100","kcfap100","kcfap200","kcfpp100","kcglp100","kcglp200","kcgmp100","kcgpp100","kcrap100","kcrap200","mflbp100","mfltp100","mftxp100","mfwop100","mfwop10h","mfwop300","mfwop30h","mfwop400","mscmp100","mspmp100","mspmpext","msvmp100","obcdp100","obcdp200","obcop100","obcop200","obcop300","obcopext","obcrp100","obcrpext","obirp111","obotp100","obstp100","obtmp100","obtop100","popvp100","potmp100","pspsp100","sasmp100"]
for i in range(len(ll)):
    print("val df_schema = sqlContext.read.format(\"com.databricks.spark.csv\").option(\"header\", \"true\").option(\"inferSchema\", \"true\").option(\"charset\", \"UTF-8\").load(\"s3n://fragrancedatalake/FragranceAugust2017/" + ll[i] + ".csv\")")
    print("\n")
    print("df_schema.printSchema()")
    print("\n \n \n")
    print("val     newNames = Seq()")
    print("val     dfRenamed = df_schema.toDF(newNames: _ *)")

    print("dfRenamed.write.format(\"parquet\").save(\"Datalake/parquet_wo_part/" + ll[i] + "\")")

    print("\n \n \n")

    print("hdfs     dfs -copyToLocal     Datalake/parquet_wo_part/" + ll[i] +  " /mnt1/datalake/parquet_wo_part/" )
    print("aws  s3  cp  /mnt1/datalake/parquet_wo_part/" + ll[i] +  " s3://fragrancedatalake/parquet_without_partition_sep12/" + ll[i] + " --recursive")
    print("\n \n")
    print( " ########Create table in Atheeeeeeeeeeeeeena")

    print("CREATE EXTERNAL TABLE IF NOT EXISTS fragrancedatalake." + ll[i] + "( \n \n \n \n )")

    print("\n \n")

    print(" ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' ")
    print(" WITH SERDEPROPERTIES( 'serialization.format' = '1' )")
    print(" LOCATION 's3://fragrancedatalake/parquet_without_partition_sep12/" + ll[i] + "/'")
    print("TBLPROPERTIES('has_encrypted_data' = 'false')")

    """    print(" alter table test add  partition(ACTIV=1)
    location
    's3://fragrancedatalake/parquet_without_partition_sep12/'
    """
    print("%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%")
    print("\n \n")