import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.security.UserGroupInformation;

public class HbaseConfUtil {
    private String zookeeperQuorum = "xxx.xx.xxx.xxx";
    private String clientPort = "2181";
    private String znodeParent = "/hbase-secure";
    private String kerberosConfPath = "/etc/krb5.conf";
    private String principal = "spark-xxxx@XXXX.COM";
    private String keytabPath = "/etc/securty/keytabs/spark.xxxx.keytab";

    public Configuration getHbaseConf() {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", zookeeperQuorum);
        conf.set("hbase.zookeeper.property.clientPort", clientPort);
        conf.set("zookeeper.znode.parent", znodeParent);
        conf.setInt("hbase.rpc.timeout", 20000);
        conf.setInt("hbase.client.operation.timeout", 30000);
        conf.setInt("hbase.client.scanner.timeout.period", 200000);
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hbase.security.authentication", "kerberos");
        System.setProperty("javax.security.auth.useSubjectCredsOnly", "false");
        System.setProperty("java.security.krb5.conf", kerberosConfPath);

        return conf;
    }

    public synchronized Connection getConnection() throws Exception {
        Connection connection = null;
        Configuration conf = getHbaseConf();
		// 使用票据登陆Kerberos
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(principal, keytabPath);

        connection = ConnectionFactory.createConnection(conf);
        return connection;
    }

    public static void main(String[] args) throws Exception {
        HbaseConfUtil hbaseConfUtil = new HbaseConfUtil();
        Connection connection = hbaseConfUtil.getConnection();
        TableName info = TableName.valueOf("DB_TYPE_INFO");
        Table table = connection.getTable(info);

        System.out.println(table);
    }
}
