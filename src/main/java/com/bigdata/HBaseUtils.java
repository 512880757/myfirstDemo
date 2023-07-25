package com.bigdata;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;
import java.util.Iterator;

// 工具类中的方法一般都是静态方法
public class HBaseUtils {


    public static Connection connection = null;

    // static 在进入jvm虚拟机的时候就会执行 static
    static {
        Configuration configuration = HBaseConfiguration.create();
        // java 代码是通过zk 来控制 hbase的
        configuration.set("hbase.zookeeper.quorum", "caiji:2181");
        try {
            connection = ConnectionFactory.createConnection(configuration);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 获取admin对象方法
    public static Admin getAdmin() {
        Admin admin = null;
        try {
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return admin;
    }

    // 获取connection
    public static Connection getConnection() {
        return connection;
    }

    // 用完之后记得关闭
    public static void close(Admin admin) {
        try {
            admin.close();
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 给定一个名字，还给你一个table对象
    public static Table getTable(String tn) {
        TableName tableName = TableName.valueOf(tn);
        Table table = null;
        try {
            table = connection.getTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return table;
    }

    // 关闭table
    public static void closeTable(Table table) {
        try {
            // alt + enter
            if (table != null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 打印一行数据的结果数据
    public static void printResult(Result result) {

        // 得到了所有列的数据
        CellScanner cellScanner = result.cellScanner();
        while (true) {
            try {
                if (!cellScanner.advance()) break;
            } catch (IOException e) {
                e.printStackTrace();
            }
            // 就是一个单元格
            Cell cell = cellScanner.current();
            // 这个单元格属于哪个列族
            System.out.println(new String(CellUtil.cloneFamily(cell)));
            // 这个单元格所在的列
            System.out.println(new String(CellUtil.cloneQualifier(cell)));
            // 打印这一行的主键 rowKey
            System.out.println(new String(CellUtil.cloneRow(cell)));
            // 打印单元格的值
            System.out.println(new String(CellUtil.cloneValue(cell)));
        }
    }
   //打印过滤后的结果集
    public static void applyResult(Filter filter, Table table){
        Scan scan = new Scan();
        scan.setFilter(filter);
        ResultScanner resultScanner=null;
        try {
            resultScanner = table.getScanner(scan);
        } catch (IOException e) {
           e.printStackTrace();
        }
        Iterator<Result> iterator = resultScanner.iterator();
        while (iterator.hasNext()){
            Result result = iterator.next();
            printResult(result);
        }
    }


}
