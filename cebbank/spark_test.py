import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder().getOrCreate()

val spark = SparkSession
      .builder
      .appName('MySparkApp')
      .enableHiveSupport() //开启访问Hive数据, 要将hive-site.xml等文件放入Spark的conf路径
      .master('local[2]')
      .getOrCreate()
————————————————
版权声明：本文为CSDN博主「Frantic丶Lin」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/a294233897/article/details/80904305

import spark.implicits._

val file = "file:///Users/gabriela/Documents/project/test/test03.csv"

val df = spark.read.format("csv").option("header","true").load(file)

val arr = df.collect

#arr.length-1
#arr(i).length
for (i <- 0 to 3){
    for (j<- 0 to 2){
        println(arr(i)(j))
    }
}


val df2 = df.na.drop()

val res3=data1.na.fill(Map("gender"->"wangxiao222","yearsmarried"->"wangxiao567"))

# get schema table name

val csv_data = spark.read.csv("file:///D:/java_workspace/fun_test.csv") //本地文件
val csv_data = spark.read.csv("hdfs:///tmp/fun_test.csv") //HDFS文件
val csv_data = spark.read.format("csv").load("hdfs:///tmp/fun_test.csv") //另一种写法

#读取



val arr = df12.collect


val res_sql = generate_sql()

#读表

val df5 = hive.sql(res_sql)

#groupyBy

val df6 = df5.groupBy("etl_date").count()

#整理结果

val df7 = df6.withColumn("schema_name","ecr_db")
val df8 = df7.withColumn("table_name","cust_info_t")

# schema table date count

# 合并

val df10 = df7.union(df8)

# 写入table append

import org.apache.spark.sql.SaveMode
df10.coalesce(1).write.format("csv").mode(SaveMode.Overwrite).option("header","true").save(res_file)




csv_data.coalesce(1)  //设置为一个partition, 这样可以把输出文件合并成一个文件
        .write.mode(SaveMode.Overwrite)
        .format("com.databricks.spark.csv")
        .save("file:///D:/java_workspace/fun_test.csv")
————————————————
版权声明：本文为CSDN博主「Frantic丶Lin」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/a294233897/article/details/80904305

df.filter($"schema_name"=="dwmart_cis.db").show()
