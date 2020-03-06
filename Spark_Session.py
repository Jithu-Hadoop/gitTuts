from pyspark.sql import SparkSession

def  get_Spark_Session(appname1):
    spark= SparkSession.builder.appName(appname1).getOrCreate()
    return spark
