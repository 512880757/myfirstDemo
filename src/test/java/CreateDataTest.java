import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.bigdata.HBaseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
//插入数据
public class CreateDataTest {

    Table table = null;

    @Before
    public void init(){
        table = HBaseUtils.getTable("ns01:student");
    }

    @After
    public void destory(){
        HBaseUtils.closeTable(table);
    }

    @Test
    public void createDataTest() throws IOException {
        // 向表中添加数据
        List<Put> list = new ArrayList<>();

        Collections.addAll(list,
                new Put("rk1001".getBytes()).addColumn("base_info".getBytes(), "name".getBytes(), "huangzhong".getBytes()),
                new Put("rk1001".getBytes()).addColumn("base_info".getBytes(), "age".getBytes(), "60".getBytes()),
                new Put("rk1001".getBytes()).addColumn("base_info".getBytes(), "gender".getBytes(), "male".getBytes()),

                new Put("rk1002".getBytes()).addColumn("base_info".getBytes(), "name".getBytes(), "sunshangxiang".getBytes()),
                new Put("rk1002".getBytes()).addColumn("base_info".getBytes(), "age".getBytes(), "18".getBytes()),
                new Put("rk1002".getBytes()).addColumn("base_info".getBytes(), "gender".getBytes(), "female".getBytes()),
                new Put("rk1002".getBytes()).addColumn("extra_info".getBytes(), "country".getBytes(), "shu".getBytes()),

                new Put("rk1003".getBytes()).addColumn("base_info".getBytes(), "name".getBytes(), "machao".getBytes()),
                new Put("rk1003".getBytes()).addColumn("base_info".getBytes(), "gender".getBytes(), "male".getBytes()),
                new Put("rk1003".getBytes()).addColumn("extra_info".getBytes(), "country".getBytes(), "shu".getBytes()),

                new Put("rk1004".getBytes()).addColumn("base_info".getBytes(), "name".getBytes(), "caocao".getBytes()),
                new Put("rk1004".getBytes()).addColumn("base_info".getBytes(), "age".getBytes(), "58".getBytes()),
                new Put("rk1004".getBytes()).addColumn("base_info".getBytes(), "gender".getBytes(), "male".getBytes()),
                new Put("rk1004".getBytes()).addColumn("extra_info".getBytes(), "country".getBytes(), "wei".getBytes()),

                new Put("rk1005".getBytes()).addColumn("base_info".getBytes(), "name".getBytes(), "zhouyu".getBytes()),
                new Put("rk1005".getBytes()).addColumn("base_info".getBytes(), "age".getBytes(), "29".getBytes()),
                new Put("rk1005".getBytes()).addColumn("extra_info".getBytes(), "country".getBytes(), "wu".getBytes()),

                new Put("rk1006".getBytes()).addColumn("base_info".getBytes(), "name".getBytes(), "luxun".getBytes()),
                new Put("rk1006".getBytes()).addColumn("base_info".getBytes(), "age".getBytes(), "17".getBytes()),
                new Put("rk1006".getBytes()).addColumn("base_info".getBytes(), "gender".getBytes(), "male".getBytes()),
                new Put("rk1006".getBytes()).addColumn("extra_info".getBytes(), "country".getBytes(), "wu".getBytes()),

                new Put("rk1007".getBytes()).addColumn("base_info".getBytes(), "name".getBytes(), "zhangliao".getBytes()),
                new Put("rk1007".getBytes()).addColumn("base_info".getBytes(), "age".getBytes(), "48".getBytes()),
                new Put("rk1007".getBytes()).addColumn("base_info".getBytes(), "gender".getBytes(), "male".getBytes()),
                new Put("rk1007".getBytes()).addColumn("extra_info".getBytes(), "country".getBytes(), "wei".getBytes()),

                new Put("rk1008".getBytes()).addColumn("base_info".getBytes(), "name".getBytes(), "jiangwei".getBytes()),
                new Put("rk1008".getBytes()).addColumn("base_info".getBytes(), "gender".getBytes(), "male".getBytes()),
                new Put("rk1008".getBytes()).addColumn("extra_info".getBytes(), "country".getBytes(), "shu".getBytes()),

                new Put("rk1009".getBytes()).addColumn("base_info".getBytes(), "name".getBytes(), "huanggai".getBytes()),
                new Put("rk1009".getBytes()).addColumn("base_info".getBytes(), "age".getBytes(), "66".getBytes()),
                new Put("rk1009".getBytes()).addColumn("extra_info".getBytes(), "country".getBytes(), "wu".getBytes())
        );

        table.put(list);
    }
}
