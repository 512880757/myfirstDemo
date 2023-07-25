import com.bigdata.HBaseUtils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;


public class TestHBaseDemo2 {

    Admin admin = null;
    @Before
    public void init(){
        admin = HBaseUtils.getAdmin();
    }

    @After
    public void destroy(){
        HBaseUtils.close(admin);
    }


    // 查看数据库中所有的表
    @Test
    public void testListTable() throws Exception{
        TableName[] tableNames = admin.listTableNames();
        for (TableName tableName:tableNames) {
            System.out.println(tableName.getNameAsString());
        }
    }

    // 查看数据库中所有的表
    @Test
    public void testListTableByNS() throws Exception{

        // 第一种方法
        TableName[] tableNames = admin.listTableNamesByNamespace("ns01");
        for (TableName tableName:tableNames) {
            System.out.println(tableName.getNameAsString());
        }

        // 第二种方法
        List<TableDescriptor> tableDescriptors = admin.listTableDescriptorsByNamespace("ns01".getBytes());
        for (TableDescriptor td:tableDescriptors) {
            System.out.println(td.getTableName().getNameAsString());
        }
    }

    // 创建一个表   create 'ns01:teacher','base_info'
    @Test
    public void testCreateTable() throws Exception{


        //  admin 执行这个描述器
        TableName tableName = TableName.valueOf("ns01:teacher");
        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableName);
        ColumnFamilyDescriptor columnFamilyDescriptor = ColumnFamilyDescriptorBuilder.newBuilder("base_info".getBytes()).build();
        TableDescriptor tableDescriptor = tableDescriptorBuilder.setColumnFamily(columnFamilyDescriptor).build();
        admin.createTable(tableDescriptor);
    }

    // 修改列族信息
    @Test
    public void testModifyTable() throws Exception{

        TableName tableName = TableName.valueOf("ns01:teacher");
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("base_info".getBytes());
        columnFamilyDescriptorBuilder.setMaxVersions(7);
        ColumnFamilyDescriptor columnFamilyDescriptor =columnFamilyDescriptorBuilder.build();
        admin.modifyColumnFamily(tableName,columnFamilyDescriptor);
    }

    // 新增一个列族
    @Test
    public void testAddCF() throws Exception{
        TableName tableName = TableName.valueOf("ns01:teacher");
        ColumnFamilyDescriptorBuilder columnFamilyDescriptorBuilder = ColumnFamilyDescriptorBuilder.newBuilder("addr_info".getBytes());

        columnFamilyDescriptorBuilder.setMaxVersions(3);
        ColumnFamilyDescriptor columnFamilyDescriptor =columnFamilyDescriptorBuilder.build();

        admin.addColumnFamily(tableName,columnFamilyDescriptor);
    }

    // 删除列族
    @Test
    public void testDeleteCF() throws Exception{
        TableName tableName = TableName.valueOf("ns01:teacher");
        admin.deleteColumnFamily(tableName,"addr_info".getBytes());
    }

    // 删除表
    @Test
    public void testDeleteTable() throws Exception{
        TableName tableName = TableName.valueOf("ns01:teacher");
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

}