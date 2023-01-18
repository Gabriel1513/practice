
# oozie工作流

# https://blog.csdn.net/zkf541076398/article/details/79941581

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

package com.xtd.hdfs

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

object HDFSUtils {

  def main(args: Array[String]): Unit = {
    val files = getHDFSFiles("hdfs://ns1/home/data/test")
    files.foreach(println(_))
  }

  /**
   * 给定hdfs目录返回目录下文件名的集合
   * @param hdfsDirectory
   * @return Array[String]
   */
  def getHDFSFiles(hdfsDirectory:String): Array[String] ={
    val configuration:Configuration = new Configuration()
//    configuration.set("fs.defaultFS", hdfsFileName)
    val fileSystem:FileSystem = FileSystem.get(configuration)
    val fsPath: Path = new Path(hdfsDirectory)
    val iterator = fileSystem.listFiles(fsPath, true)
    val list = new ListBuffer[String]
    while (iterator.hasNext) {
      val pathStatus = iterator.next()
      val hdfsPath = pathStatus.getPath
      val fileName = hdfsPath.getName
      list += fileName // list.append(fileName)
    }
    fileSystem.close()
    list.toArray
  }

}

import java.util.Properties

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object readDataFromJDBC {
  def main(args: Array[String]): Unit = {
    val session: SparkSession = SparkSession.builder().appName("ReadDataFromJDBC").master("local[*]").getOrCreate()

    //从数据库中加载数据
    val logs: DataFrame = session.read.format("jdbc").options(
      Map("url" -> "jdbc:mysql://slave3:3306/bigdata",
        "driver" -> "com.mysql.jdbc.Driver",
        "dbtable" -> "province",
        "user" -> "root",
        "password" -> "root"
      )).load()

    // 过滤方法1， RDD方法
//    val filtered: Dataset[Row] = logs.filter(row => {
//      row.getAs[Int](1) <= 1000
//    })

    // 过滤方法2， lambda表达
    import session.implicits._
    val filtered: Dataset[Row] = logs.filter($"num" <= 1000)

    // 将过滤后的数据写入新的表,新表可以不存在
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "root")
    // mode 参数： ignore 若表存在，不作任何处理； overwrite 表示覆盖  append 表示追加
    filtered.write.mode("ignore").jdbc("jdbc:mysql://slave3:3306/bigdata", "filter_province", properties)

    filtered.show()


    session.stop()

  }

————————————————
版权声明：本文为CSDN博主「静远小和尚」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/qq_29672495/article/details/107370678


## 也需要把 hive/conf/hive-site.xml 复制到 spark/conf 下
spark = SparkSession.builder.appName('test').master('yarn').enableHiveSupport().getOrCreate()

hive_data = spark.sql('select * from hive1101.person limit 2')

# 创建spark session
spark = SparkSession.builder.appName(
     "test").enableHiveSupport().getOrCreate()
# df 转为临时表/临时视图
df.createOrReplaceTempView("df_tmp_view")
# spark.sql 插入hive
spark.sql(""insert overwrite table
                    XXXXX  # 表名
                   partition(分区名称 =分区值)   # 多个分区按照逗号分开
                   select
                   XXXXX  # 字段名称，跟hive字段顺序对应，不包含分区字段
                   from df_tmp_view"")

————————————————
版权声明：本文为CSDN博主「SummerHmh」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/SummerHmh/article/details/103820628

df.write.mode('overwrite')\
.partitionBy("channel","event_day","event_hour")\  # 分区字段逗号分割，跟建表顺序一致
.saveAsTable("table_name")  # 如果首次执行不存在这个表，会自动创建分区表，不指定分区即创建不带分区的表
————————————————
版权声明：本文为CSDN博主「SummerHmh」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/SummerHmh/article/details/103820628

# 可动态分区设置
spark.sql("set hive.exec.dynamic.partition.mode=nonstrict;")
# df 按照字段+分区的顺序对应hive顺序
df.write\
.insertInto("table_name")  # 如果执行不存在这个表，会报错
————————————————
版权声明：本文为CSDN博主「SummerHmh」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/SummerHmh/article/details/103820628

df.write().mode("overwrite").saveAsTable("tableName");
或
df.select(df.col("col1"),df.col("col2")) .write().mode("overwrite").saveAsTable("schemaName.tableName");
或
df.write().mode(SaveMode.Overwrite).saveAsTable("dbName.tableName");

#https://blog.csdn.net/hellozhxy/article/details/83576016

#https://blog.csdn.net/u011702633/article/details/86646773
