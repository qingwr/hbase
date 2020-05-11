package com.dasi.bigData.hbase.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * java hbase api 使用
 * 几个主要 Hbase API 类和数据模型之间的对应关系：
 * 1、 HBaseAdmin
 * 关系： org.apache.hadoop.hbase.client.HBaseAdmin
 * 作用：提供了一个接口来管理 HBase 数据库的表信息。
 * 它提供的方法包括：创建表，删 除表，列出表项，使表有效或无效，以及添加或删除表列族成员等。
 * 2、 HBaseConfiguration
 * 关系： org.apache.hadoop.hbase.HBaseConfiguration
 * 作用：对 HBase 进行配置
 * 3、 HTableDescriptor
 * 关系： org.apache.hadoop.hbase.HTableDescriptor
 * 作用：包含了表的名字其对应表的列族
 * 4、 HColumnDescriptor
 * 关系： org.apache.hadoop.hbase.HColumnDescriptor
 * 作用：维护着关于列族的信息，例如版本号，压缩设置等。它通常在创建表或者为表添 加列族的时候使用。列族被创建后不能直接修改，只能通过删除然后重新创建的方式。
 * 列族被删除的时候，列族里面的数据也会同时被删除。
 * 5、 HTable
 * 关系： org.apache.hadoop.hbase.client.HTable
 * 作用：可以用来和 HBase 表直接通信。此方法对于更新操作来说是非线程安全的。
 * 6、 Put
 * 关系： org.apache.hadoop.hbase.client.Put
 * 作用：用来对单个行执行添加操作
 * 7、 Get
 * 关系： org.apache.hadoop.hbase.client.Get
 * 作用：用来获取单个行的相关信息
 * 8、 Result
 * 关系： org.apache.hadoop.hbase.client.Result
 * 作用：存储 Get 或者 Scan 操作后获取表的单行值。使用此类提供的方法可以直接获取值 或者各种 Map 结构（ key-value 对）
 */
public class BaseAPIDemo {

    protected static Connection conn;
    private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    private static final String HBASE_POS = "192.168.0.180";
    private static final String ZK_POS = "node1:2181,node2:2181,node3:2181";
    private static final String ZK_PORT_VALUE = "2181";
    private static final Configuration conf = HBaseConfiguration.create();
    private static Admin admin = null;


    /*** 静态构造，在调用静态方法前运,初始化连接对象  * */
    static {
        conf.set("hbase.rootdir", "hdfs://" + HBASE_POS + ":9000/hbase");
        conf.set(ZK_QUORUM, ZK_POS);
        conf.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);
        try {
            conn = ConnectionFactory.createConnection(conf);
            admin = conn.getAdmin();
        } catch (Exception e) {
            System.out.println("创建连接池失败!");
            e.printStackTrace();
        }
    }


    public static void main(String[] args) throws Exception {
        //createTable("hbaseTable");
        //listTable();
        //descTable("myHbase");
        //disableTabl("myHbase");
        //dropTable("myHbase");
        //modifyTable("student");
        //putData("student", "10000000", "info", "age", "1000");
        printResult("student", "10000000");
    }

    /**
     * 创建表
     *
     * @param tableName 表名
     */
    public static void createTable(String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            System.out.println("table已经存在！");
            return;
        }
        // 表定义实例
        HTableDescriptor htd = new HTableDescriptor(name);
        // 列族定义实例
        HColumnDescriptor hcd = new HColumnDescriptor("info");
        // 添加列族到表中
        htd.addFamily(hcd);
        //创建表
        admin.createTable(htd);
    }

    /**
     * 判断表是否存在
     *
     * @param tableName 表名
     */
    public static boolean isExistTable(String tableName) throws IOException {
        return admin.tableExists(TableName.valueOf(tableName));
    }

    /**
     * 查看所有表
     */
    public static void listTable() throws IOException {
        HTableDescriptor[] listTables = admin.listTables();
        HColumnDescriptor[] listColumns = null;
        for (HTableDescriptor table : listTables) {
            System.out.println("table name: " + table.getNameAsString());
            listColumns = table.getColumnFamilies();
            for (HColumnDescriptor column : listColumns) {
                System.out.print("\t" + "columnFamilyName:" + column.getNameAsString());
            }
            System.out.println();
        }
    }

    /*
     * 查看表的列簇属性
     * @Param tableName
     * */
    public static void descTable(String tableName) throws IOException {
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            System.out.println("table name:" + tableName);
            HTableDescriptor table = admin.getTableDescriptor(name);
            for (HColumnDescriptor col : table.getColumnFamilies()) {
                System.out.println("\t ColumnFamili : " + col.getNameAsString());
            }
            return;
        }
        System.out.println(tableName + "不存在");
    }


    /**
     * 使表不可用
     * disable table
     * enable table 使表可用
     *
     * @param tableName 表名
     */
    public static void disableTabl(String tableName) throws IOException {
        TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            admin.disableTable(table);
            return;
        }
        System.out.println(tableName + "不存在");
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    public static void dropTable(String tableName) throws IOException {
        TableName table = TableName.valueOf(tableName);
        if (admin.tableExists(table)) {
            Boolean isEnabled = admin.isTableEnabled(table);
            System.out.println(tableName + " 是否可用 " + isEnabled);
            if (isEnabled) {
                admin.disableTable(table);
            }
            admin.deleteTable(table);
            if (admin.tableExists(table)) {
                System.out.println(tableName + "删除失败");
            } else {
                System.out.println(tableName + "删除成功");
            }
            return;
        }
        System.out.println(tableName + "不存在");
    }

    /**
     * 添加列族
     */
    public static void modifyTable(String tableName) throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //判断表是否存在
        if (admin.tableExists(name)) {
            //判断表是否可用状态
            boolean tableEnabled = admin.isTableEnabled(name);

            if (tableEnabled) {
                //使表变成不可用
                admin.disableTable(name);
            }
            //根据表名得到表
            HTableDescriptor tableDescriptor = admin.getTableDescriptor(name);
            //创建列簇结构对象
            HColumnDescriptor columnFamily1 = new HColumnDescriptor("cf3".getBytes());
            HColumnDescriptor columnFamily2 = new HColumnDescriptor("cf4".getBytes());

            tableDescriptor.addFamily(columnFamily1);
            tableDescriptor.addFamily(columnFamily2);
            //添加列族
            admin.modifyTable(name, tableDescriptor);

        } else {
            System.out.println("table不存在");
        }
    }

    /**
     * 添加数据
     * tableName:    表明
     * rowKey:    行键
     * familyName:列簇
     * columnName:列名
     * value:        值
     */
    public static void putData(String tableName, String rowKey, String familyName, String columnName, String value)
            throws Exception {
        //转化为表名
        TableName name = TableName.valueOf(tableName);
        //添加数据之前先判断表是否存在，不存在的话先创建表
        if (!admin.tableExists(name)) {
            //根据表明创建表结构
            HTableDescriptor tableDescriptor = new HTableDescriptor(name);
            //定义列簇的名字
            HColumnDescriptor columnFamilyName = new HColumnDescriptor(familyName);
            tableDescriptor.addFamily(columnFamilyName);
            admin.createTable(tableDescriptor);
        }
        Table table = conn.getTable(name);
        Put put = new Put(rowKey.getBytes());
        put.addColumn(familyName.getBytes(), columnName.getBytes(), value.getBytes());
        //制定版本号
        //put.addImmutable(familyName.getBytes(), columnName.getBytes(), timestamp, value.getBytes());
        table.put(put);
    }

    // 根据rowkey查询数据
    public static void printResult(String tableName, String rowKey) throws Exception {
        Result result;
        TableName name = TableName.valueOf(tableName);
        if (admin.tableExists(name)) {
            Table table = conn.getTable(name);
            Get get = new Get(rowKey.getBytes());
            result = table.get(get);
        } else {
            result = null;
        }
        for (Cell cell : result.listCells()) {
            System.out.println("cell = " + cell.toString());
        }
    }


}
