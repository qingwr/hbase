package com.dasi.bigData.hbase.core;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import java.io.IOException;

/**
 * java hbase api 使用
 * */
public class BaseAPIDemo {

    protected static Connection conn;
    private static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    private static final String ZK_CLIENT_PORT = "hbase.zookeeper.property.clientPort";
    private static final String HBASE_POS = "192.168.0.180";
    private static final String ZK_POS = "node1:2181,node2:2181,node3:2181";
    private static final String ZK_PORT_VALUE = "2181";

    /*** 静态构造，在调用静态方法前运行，  初始化连接对象  * */
    static {
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://" + HBASE_POS + ":9000/hbase");
        conf.set(ZK_QUORUM, ZK_POS);
        conf.set(ZK_CLIENT_PORT, ZK_PORT_VALUE);
        //创建连接池
        try {
            conn = ConnectionFactory.createConnection(conf);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }


    public static void main(String[] args) throws IOException {
        listTable();
    }

    /**
     * 查看所有表
     * */
    public static void listTable() throws IOException {
        Admin admin = conn.getAdmin();
        for (TableName tab : admin.listTableNames()) {
            System.out.println("tableName = " + tab.getNameAsString());
        }
    }

}
