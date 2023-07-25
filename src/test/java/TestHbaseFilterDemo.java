import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import com.bigdata.HBaseUtils;

import java.io.IOException;
import java.util.Iterator;

public class TestHbaseFilterDemo {
    Table table;

    @Before//初始化
    public void init() {
        table = HBaseUtils.getTable("ns01:student");
    }

    @After
    public void destory() {
        HBaseUtils.closeTable(table);
    }

    @Test
    public void testSingleColoumFilter() throws IOException {
        //单值过滤器 SingleColumnValueFilter
        // where  family='base_info' and age<30
        SingleColumnValueFilter singleColumnValueFilter =
                new SingleColumnValueFilter(
                        "base_info".getBytes(),
                        "age".getBytes(),
                        CompareOperator.LESS,//小于  greater：大于
                        "30".getBytes());
        //过滤掉一些没有年龄也查询出来的数据
        singleColumnValueFilter.setFilterIfMissing(true);
        Scan scan = new Scan();
        //select *from stident
        //添加条件
        scan.setFilter(singleColumnValueFilter);
        //select * from stident where  family='base_info' and age<30
        ResultScanner resultScanner = table.getScanner(scan);
        //迭代器 Iterator这是一个接口类 集合一般都实现这个接口
        Iterator<Result> iterator = resultScanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            HBaseUtils.printResult(result);
        }
    }

    //多个过滤条件
    // 查询年龄小于30岁，蜀国，女的
    @Test
    public void testMoreColoumFilter() throws IOException {
        //单值过滤器 SingleColumnValueFilter
        // where  family='base_info' and age<30
        SingleColumnValueFilter singleColumnValueFilter =
                new SingleColumnValueFilter(
                        "base_info".getBytes(),
                        "age".getBytes(),
                        CompareOperator.LESS,//小于  greater：大于
                        "30".getBytes());
        //过滤掉一些没有年龄也查询出来的数据
        singleColumnValueFilter.setFilterIfMissing(true);
        //过滤出等于蜀国的数
        SingleColumnValueFilter singleColumnValueFilter2 =
                new SingleColumnValueFilter(
                        "extra_info".getBytes(),
                        "country".getBytes(),
                        CompareOperator.EQUAL,//小于  greater：大于  EQUAL等于
                        "shu".getBytes());
        //过滤掉不是蜀国也显示的数据
        singleColumnValueFilter2.setFilterIfMissing(true);
        //过滤出  female 的数据
        SingleColumnValueFilter singleColumnValueFilter3 =
                new SingleColumnValueFilter(
                        "base_info".getBytes(),
                        "gender".getBytes(),
                        CompareOperator.EQUAL,//小于  greater：大于  EQUAL等于
                        "female".getBytes());
        //过滤掉不是female 也显示的数据
        singleColumnValueFilter3.setFilterIfMissing(true);
        // new 一个FilterList 过滤规则的集合  将过滤条件添加到里面
        FilterList filterList = new FilterList();
        filterList.addFilter(singleColumnValueFilter);
        filterList.addFilter(singleColumnValueFilter2);
        filterList.addFilter(singleColumnValueFilter3);
        // new 一个获取的多条数据的scan
        Scan scan = new Scan();
        //添加条件
        //多态
        scan.setFilter(filterList);
        //select * from stident where  family='base_info' and age<30
        ResultScanner resultScanner = table.getScanner(scan);
        //迭代器 Iterator这是一个接口类 集合一般都实现这个接口
        Iterator<Result> iterator = resultScanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            HBaseUtils.printResult(result);
        }
    }

    //过滤器中使用比较器
    @Test
    public void testCompartor() throws IOException {
        //使用二进制比较器名字是zhouyu的数据  用于等值比较
       //BinaryComparator binaryComparator = new BinaryComparator("zhouyu".getBytes());
         //正则表达式比较
        //RegexStringComparator regexStringComparator = new RegexStringComparator("^huang");
        //相同字节的前缀比较
        BinaryPrefixComparator binaryPrefixComparator = new BinaryPrefixComparator("huang".getBytes());
        //字串比较器
        SubstringComparator substringComparator = new SubstringComparator("yu");
        SingleColumnValueFilter singleColumnValueFilter =
                new SingleColumnValueFilter(
                        "base_info".getBytes(),
                        "name".getBytes(),
                        CompareOperator.EQUAL, binaryPrefixComparator);//小于  greater：大于  EQUAL等于
        Scan scan = new Scan();
        scan.setFilter(singleColumnValueFilter);
        ResultScanner resultScanner = table.getScanner(scan);
        //迭代器 Iterator这是一个接口类 集合一般都实现这个接口
        Iterator<Result> iterator = resultScanner.iterator();
        while (iterator.hasNext()) {
            Result result = iterator.next();
            HBaseUtils.printResult(result);

        }
    }
    //其他过滤器  列族过滤器
    @Test
    public void testFamilyFilter() throws IOException {
        //查询与info结尾的列族过滤器
        FamilyFilter familyFilter = new FamilyFilter(CompareOperator.EQUAL,new RegexStringComparator("info$"));
       HBaseUtils.applyResult(familyFilter,table);
    }
    //列名过滤器
    @Test
    public void testQualifierFilter() throws IOException {
        //过滤列名为gender
       // QualifierFilter qualifierFilter = new QualifierFilter(CompareOperator.EQUAL, new BinaryComparator("gender".getBytes()));
        //列名中含a字符的数据
        QualifierFilter qualifierFilter = new QualifierFilter(CompareOperator.EQUAL, new SubstringComparator("a"));
        HBaseUtils.applyResult(qualifierFilter,table);
    }
    //列名前缀过滤器
    @Test
    public void testColumnPrefixFilter() throws IOException {
        // 获取列名以ge开头的数据
        ColumnPrefixFilter columnPrefixFilter = new ColumnPrefixFilter("ge".getBytes());
        HBaseUtils.applyResult(columnPrefixFilter,table);
    }
    //多列名前缀
    @Test
    public void testMultipleColumnPrefixFilter() throws IOException {
        // 获取列名以a开头或者以b开头的数据
        MultipleColumnPrefixFilter multipleColumnPrefixFilter =
                new MultipleColumnPrefixFilter(new byte[][]{"a".getBytes(),"b".getBytes()});
        HBaseUtils.applyResult(multipleColumnPrefixFilter,table);
    }
    //列名范围过滤器
    @Test
    public void testColumnRangeFilter() throws IOException {
        // 获取列名以a开头的数据 或者b 开头的数据
        ColumnRangeFilter columnRangeFilter =
        new ColumnRangeFilter("age".getBytes(),true,"gender".getBytes(),false);
        HBaseUtils.applyResult(columnRangeFilter ,table);

        /**
         *     base_info         extra_info
         *    age gender name    country
         *
         *    select age~name from ns01:student ;
         */

    }
    //行值过滤器
    @Test
    public void testRoweFilter() throws IOException {
        // 查询 主键中， 含有 rkxxx1  rkxxx4  rkxxx7
        // 正则表达式   以^ 开头 以 $结尾  \d 表示数字[0-9]  {3}前方出现三次  [147] 表示这个位置 只能是 1 4 7
        RowFilter rowFilter =
        new RowFilter(CompareOperator.EQUAL,new RegexStringComparator("^rk\\d{3}[147]$"));
         HBaseUtils.applyResult( rowFilter,table);
    }
    //单行单列过滤器
    @Test
    public void testFirstKeyOnlyFilter() throws IOException {
        FirstKeyOnlyFilter firstKeyOnlyFilter = new FirstKeyOnlyFilter();
        HBaseUtils.applyResult( firstKeyOnlyFilter,table);
    }
    //分页过滤器
    @Test
    public void testpageFilter() throws IOException {
        //查看第一页的数据
        PageFilter pageFilter = new PageFilter(5);
        HBaseUtils.applyResult( pageFilter,table);
        //查看第二页数据  需要使用scan
        Scan scan = new Scan();
        scan.withStartRow("rk1006".getBytes());
        scan.setFilter(pageFilter);
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> iterator = scanner.iterator();
        while (iterator.hasNext()){
            Result next = iterator.next();
            HBaseUtils.printResult(next);

        }
    }
    @Test
    public void testPageFilter2() throws IOException {
        // 分页查询
        PageFilter pageFilter = new PageFilter(3);// 每一页是多少条数据
        Scan scan = new Scan().setFilter(pageFilter);

        byte[] beginKey = null;
        while(true){
            int count = 0 ;// 表示本次查询出来的条数，如果条数小于 3 说明是最后一页了
            ResultScanner scanner = table.getScanner(scan);
            for (Result result:scanner) {

                HBaseUtils.printResult(result);
                count++;
                beginKey = result.getRow();
            }
            System.out.println("---------------------------------------");
            if(count < 3){
                break; // 跳出死循环
            }
            // 第二个参数的意思是是否包含当前的key值   rk1005
            scan.withStartRow(beginKey,false);
        }

    }

}