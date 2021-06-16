
# JDBC 写入

from pyspark.sql import SQLContext, SparkSession

spark = SparkSession.builder.master("local").appName("testdemo").getOrCreate()

ctx = SQLContext(spark)

d1 = [{'name':'Alice','age':1}]

df = ctx.createDataFrame(d1)

df.write.jdbc(url, table='actor1',mode=append)


# parquet

from pyspark.sql import SparkSession, HiveContext,DataFrameWriter
import argparse
import time
import numpy as np
import pandas as pd


spark = SparkSession.builder.enableHiveSupport().appName("test").getOrCreate()

start = time.time()



### 数据载入方法1： hdfs上载入parquent格式

input = "/aaa/bbb/ccc"

data = spark.read.parquet(input)


from pyspark.sql import SparkSession, HiveContext,DataFrameWriter


spark = SparkSession.builder.enableHiveSupport().appName("test").getOrCreate()

url = ""

data = spark.read.parquet(input)


url = "jdbc:postgresql://localhost/foobar"

properties = {
    "user": "foo",
    "password": "bar"
}

df.write.jdbc(url=url, table="baz", mode=mode, properties=properties)


sqlContext.read.jdbc(url=url, table="baz", properties=properties)

(sqlContext.read.format("jdbc")
    .options(url=url, dbtable="baz", **properties)
    .load())

ctx=SQLContext(spark)
jdbcdf=ctx.read.format("jdbc").options(url=url,driver=driver,dbtable='t_datav',user=user,password=password).load()

jdbcDF = ctx.read.format("jdbc").option("url", url).option("driver", driver1).option("dbtable", table).option( "user", user).option("password", password).load()
jdbcDF.show()

#JDBC连接数据库



jdbcDF = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql:dbserver") \
    .option("dbtable", "schema.tablename") \
    .option("user", "username") \
    .option("password", "password") \
    .load()

from pyspark.sql import SQLContext
from pyspark import SparkConf,SparkContext

masterurl='spark://master:7077'
# conf = SparkConf().setAppName("miniProject").setMaster("local[*]").set("spark.sql.execution.arrow.enabled", "true")
conf = SparkConf().setAppName("miniProject").setMaster(masterurl).set("spark.sql.execution.arrow.enabled", "true")
sc = SparkContext.getOrCreate(conf)
sql_context = SQLContext(sc)
url="jdbc:mysql://localhost:3306/test"
driver= 'com.mysql.cj.jdbc.Driver'
user='base'
password='****'
table='t_datav'
properties = {"user":user,"password":password}
df = sql_context.read.jdbc(url=url, table=table, properties=properties)
df.show()



from pyspark.sql import SQLContext,SparkSession

urll='spark://master:7077'
url="jdbc:mysql://localhost:3306/test"
driver = 'com.mysql.cj.jdbc.Driver'
user='base'
password='*****'
table='t_datav'
properties={'user':user,'password':password}
spark=SparkSession.builder\
    .master(urll)\
    .appName("testdemo")\
    .getOrCreate()
ctx=SQLContext(spark)
jdbcdf=ctx.read.format("jdbc").options(url=url,driver=driver,dbtable='t_datav',user=user,password=password).load()
jdbcdf.show()
jdbcdf.printSchema()
