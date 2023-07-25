import com.bigdata.HBaseUtils;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;

public class TestHBaseDemo3 {

    Table table = null;

    @Before
    public void init() {
        table = HBaseUtils.getTable("ns01:teacher3");
    }

    @After
    public void destroy() {
        HBaseUtils.closeTable(table);
    }


    // 插入数据
    @Test
    public void testInsertData() throws Exception {
        // 需要一个Table 对象
        // table 对象 需要Put对象
        // 第一版，没有使用工具类
        /*Connection connection = HBaseUtils.getConnection();
        TableName tableName = TableName.valueOf("ns01:teacher3");
        Table table = connection.getTable(tableName);
        Put put = new Put("rk001".getBytes());
        put.addColumn("base_info".getBytes(),"name".getBytes(),"laoyan".getBytes());
        put.addColumn("base_info".getBytes(),"age".getBytes(),"19".getBytes());
        put.addColumn("addr_info".getBytes(),"addr".getBytes(),"洛阳".getBytes());
        table.put(put);
        table.close();*/
        Put put = new Put("rk001".getBytes());
        put.addColumn("base_info".getBytes(), "name".getBytes(), "laoyan".getBytes());
        put.addColumn("base_info".getBytes(), "age".getBytes(), "19".getBytes());
        put.addColumn("addr_info".getBytes(), "addr".getBytes(), "洛阳".getBytes());
        table.put(put);
    }

    // 插入数据
    @Test
    public void testBatchInsertData() throws Exception {

        Put put = new Put("rk003".getBytes());
        put.addColumn("base_info".getBytes(), "name".getBytes(), "闫哥".getBytes());
        put.addColumn("base_info".getBytes(), "age".getBytes(), "23".getBytes());
        put.addColumn("addr_info".getBytes(), "addr".getBytes(), "洛阳2".getBytes());

        Put put2 = new Put("rk002".getBytes());
        put2.addColumn("base_info".getBytes(), "name".getBytes(), "laowang".getBytes());
        put2.addColumn("base_info".getBytes(), "age".getBytes(), "36".getBytes());
        put2.addColumn("addr_info".getBytes(), "addr".getBytes(), "平顶山".getBytes());

        /*List<Put> list =new ArrayList<Put>();
        list.add(put);
        list.add(put2);*/
        // Arrays 是 数组的工具类，集合的工具类是 Collections
        // asList 可以将一个个的数据，变为List集合，这个集合只能查看，不能删除
        table.put(Arrays.asList(put, put2));
    }

    // 演示使用 scan 查看数据  scan 'ns01:teacher3'
    @Test
    public void testScanData() throws Exception {

        Scan scan = new Scan();
        ResultScanner resultScanner = table.getScanner(scan);
        // 类似 jdbc
        // 得到了所有的行
        Iterator<Result> iterator = resultScanner.iterator();
        while (iterator.hasNext()) {
            // 得到一行数据
            Result result = iterator.next();
            HBaseUtils.printResult(result);
        }
    }
}