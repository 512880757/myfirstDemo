import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class TestConnection {

    //通过判断一个表是否存在，来测试连接是否正常
    public static void main(String[] args) throws Exception {
        // 1、获取文件对象
        Configuration configuration = HBaseConfiguration.create();
        //java 代码是通过zk 来控制的hbase的
        configuration.set("hbase.zookeeper.quorum", "caiji:2181");
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        TableName tableName = TableName.valueOf("ns01:teacher");
        boolean b = admin.tableExists(tableName);
        System.out.println("该表是否存在：" + b);
        admin.close();
        connection.close();
    }
}