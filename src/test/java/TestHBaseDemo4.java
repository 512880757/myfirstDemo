import com.bigdata.HBaseUtils;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

public class TestHBaseDemo4 {

    Table table = null;

    @Before
    public void init() {
        table = HBaseUtils.getTable("ns01:teacher3");
    }

    @After
    public void destroy() {
        HBaseUtils.closeTable(table);
    }

    // 演示使用 get 查看 一条数据
    @Test
    public void testGetData() throws Exception {

        Get get = new Get("rk001".getBytes());
        Result result = table.get(get);
        HBaseUtils.printResult(result);
    }

    // 可以查看多条数据
    @Test
    public void testGetDatas() throws Exception {

        Get get = new Get("rk001".getBytes());
        Get get1 = new Get("rk002".getBytes());
        Get get2 = new Get("rk003".getBytes());
        Result[] results = table.get(Arrays.asList(get, get1, get2));
        for (Result result : results) {
            HBaseUtils.printResult(result);
        }
    }

    // 删除一条数据
    @Test
    public void testDelete() throws Exception {

        Delete delete = new Delete("rk003".getBytes());
        table.delete(delete);
    }

    // 删除一条数据中的一列数据
    // 删除一行数据中的多列，使用addColumn
    // 删除多行数据中的不同的列，需要使用多个Delete
    @Test
    public void testDeleteCell() throws Exception {

        Delete delete = new Delete("rk002".getBytes());
        delete.addColumn("base_info".getBytes(), "age".getBytes());
        delete.addColumn("addr_info".getBytes(), "addr".getBytes());
        table.delete(delete);
    }
}