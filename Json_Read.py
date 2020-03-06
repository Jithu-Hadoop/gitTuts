from Spark_Session import get_Spark_Session
from pyspark.sql import SparkSession,DataFrame
from pyspark.sql.functions import array_contains
from pyspark.sql.types import *

from pyspark.sql import Column
from pyspark.sql import functions as F
import numpy as np
def main():
    Spark = get_Spark_Session("Json_reabd")
    S=SparkSession.builder.getOrCreate()
    schema=StructType([StructField("name",StringType()),StructField("age",IntegerType()),StructField("cars",StructType
    ([StructField("car1", StringType()) ,StructField("car2", StringType()),StructField("car3", StringType())]))])
    Json_Df=Spark.read.option("multiline","true").schema(schema).json("d:/Nested_Json.json")
    Json_Df1=Json_Df
    Json_Df1.show()
    for col_name in Json_Df.columns:
        print col_name

    for i in Json_Df.select("cars.*").columns:
        c_name="cars."+i
        Json_Df=  Json_Df.withColumn(i,col=F.col(c_name))

    structureSchema = StructType().add("id", StringType()).add("dept", StringType()).add("properties",   StructType().add("salary", IntegerType()).add("location", StringType()))
    print (structureSchema)
    Json_Df6=Spark.read.option("multiline","true").schema(schema).json("d:/Nested_Json.json")
    l=[]
    for i in  Json_Df6.select("cars.*").schema.names:
        l.append(F.col(i))

    print l
    Json_Df6.createOrReplaceTempView("god")
    print Spark.sql("select   name,age,array(cars.*) as dd from god").filter(array_contains(F.col("dd"),"BMW")).show()

       # val
       # index = nested_json_df.schema.fieldIndex("cars")
       # val
       # Schema = nested_json_df.schema(index).dataType.asInstanceOf[StructType]
       # var
       # columns = LinkedHashSet[Column]()
       # Schema.fields.foreach(field= > {
        #    columns.add(col("cars." + field.name))
        #})


if __name__ == '__main__':
    main()

