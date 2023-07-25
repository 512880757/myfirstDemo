import com.bigdata.HBaseUtils;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 1、需要导入junit 包 （Junit是单元测试的技术）
 * 2、编写测试代码，在 test目录下
 * 3、编写单元测试，方法必须是 public ,返回值必须是void, 方法没有参数
 * 4、单元测试方法上要加注解 @Test  有点类似于 main方法
 * main方法的缺点是一个文件，只能有一个main方法，单元测试文件，一个文件可以有N多个测试方法
 * 5、单元测试中有三个注解  @Test @Before @After
 */
public class TestHBaseDemo {
    Admin admin = null;

    @Before
    public void init() {
        admin = HBaseUtils.getAdmin();
    }

    @After
    public void destroy() {
        HBaseUtils.close(admin);
    }

    @Test
    public void testTools() throws Exception {
        TableName tableName = TableName.valueOf("ns01:teacher");
        boolean b = admin.tableExists(tableName);
        System.out.println("该表是否存在：" + b);
    }

    //创建ns
    @Test
    public void testCreateNameSpace() throws IOException {
        NamespaceDescriptor ns02 = NamespaceDescriptor.create("ns02").build();
        admin.createNamespace(ns02);
    }

    //展示所有数据库
    @Test
    public void testlistNameSpace() throws IOException {
        NamespaceDescriptor[] namespaceDescriptors = admin.listNamespaceDescriptors();
        for (NamespaceDescriptor ns : namespaceDescriptors) {
            System.out.println(ns.getName());
        }

    }
    //修改数据库属性
    @Test
    public void testModifyNameSpace() throws IOException {
           NamespaceDescriptor ns02 =NamespaceDescriptor.create("ns02").build();
           ns02.setConfiguration("author","long");
           Date date=new Date();
           SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
           ns02.setConfiguration("createTime",format.format(date));
           //ctrl+p 显示一个方法需要的参数
        admin.modifyNamespace(ns02);
    }
    //删除数据库
    @Test
    public void testDeleteNameSpace() throws IOException {
        admin.deleteNamespace("ns02");
    }

}
