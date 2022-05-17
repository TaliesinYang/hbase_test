package com.atguigu.demo03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class Hbase_DML {
    private static Connection connection=null;
    public static Connection getConnection(){
        if (connection==null){
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            try {
                connection=ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }
    public static void closeConnection(){
        if (connection!=null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    public static void pullCell(){}
    public static void getCell(){}
    public static void delectCells(){}
    public static void scanTable(){}

}
