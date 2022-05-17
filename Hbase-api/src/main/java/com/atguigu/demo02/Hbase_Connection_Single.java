package com.atguigu.demo02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class Hbase_Connection_Single {
    private static Connection connection = null;
    public static Connection getConnection(){
        try{
            if(connection==null){
                Configuration conf = new Configuration();
                conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
                connection=ConnectionFactory.createConnection(conf);
            }

        }catch (Exception e){
            e.printStackTrace();
        }
        return connection;
    }
    public static void closeConncetion(){
        try{
            if (connection!=null)
                connection.close();
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        Connection connection = Hbase_Connection_Single.getConnection();
        System.out.println(connection);
        Hbase_Connection_Single.closeConncetion();
    }

}
