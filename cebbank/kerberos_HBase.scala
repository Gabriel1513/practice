
import java.io.IOException
import com.google.protobuf.ServiceException
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{HBaseAdmin, HTable}
import org.apache.hadoop.hbase.mapreduce.{TableInputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ClassName SparkHBase_berberos
 * @Description TODO
 * @Autor yanni
 * @Date 2020/12/16 14:46
 * @Version 1.0
 **/
object SparkHBase_berberos {

  def main(args: Array[String]): Unit = {
  	//加载krb5.conf文件
    System.setProperty("java.security.krb5.conf", "/home/sgbigdata/keytab/krb5.conf")

	//获取sparkSession
    val sparkSession = SparkSession.builder().appName(this.getClass.getSimpleName.filter(!_.equals('$'))).getOrCreate()

    //获取sparkContext
    val sparkContext = sparkSession.sparkContext

    //设置日志级别
    sparkContext.setLogLevel("WARN")

    //指定HBASE的表
    val tableName = "DWD_AMR_GS_METER-E-CURVE_201902"

    //设置HBaseConfiguration
    val hbaseConf = HBaseConfiguration.create()
    //设备zookeeper集群地址
    hbaseConf.set("hbase.zookeeper.quorum","10.213.111.XXX,10.213.111.XXX,10.213.111.XXX")
    //设置zookeeper端口
    hbaseConf.set("hbase.zookeeper.property.clientPort","2181")
    //设置要读取的表名
    hbaseConf.set(TableInputFormat.INPUT_TABLE,tableName)
    hbaseConf.set("hadoop.security.authentication", "Kerberos")

	//登录kerberos
    UserGroupInformation.setConfiguration(hbaseConf)
    try {
      UserGroupInformation.loginUserFromKeytab("YJ00004", "/home/sgbigdata/keytab/YJ00004.keytab")
      HBaseAdmin.checkHBaseAvailable(hbaseConf)
    } catch {
      case e: IOException =>
        e.printStackTrace()
      case e: ServiceException =>
        e.printStackTrace()
    }

	//读取表,获取RDD
    val hbaseRdd = sc.newAPIHadoopRDD(hbaseConf, classOf[TableInputFormat],
    				classOf[org.apache.hadoop.hbase.io.ImmutableBytesWritable],
    				classOf[org.apache.hadoop.hbase.client.Result])

    //遍历数据,转换成dateFrame类型
	val DataDF = hbaseRDD.map(result=>(
            Bytes.toDouble(result._2.getValue(Bytes.toBytes("DATA"),Bytes.toBytes("meterID-1"))),
            Bytes.toDouble(result._2.getValue(Bytes.toBytes("DATA"),Bytes.toBytes("meterID-2")))
        )).toDF("meterID-1","meterID-2")

    //注册临时表
    DataDF.createTempView("DLZZ")
    sparkSession.sql("select * from DLZZ").show()

    //关闭sparkSession
    sparkSession.stop()
  }
}
————————————————
版权声明：本文为CSDN博主「我在北国不背锅」的原创文章，遵循CC 4.0 BY-SA版权协议，转载请附上原文出处链接及本声明。
原文链接：https://blog.csdn.net/weixin_44455388/article/details/102619334
