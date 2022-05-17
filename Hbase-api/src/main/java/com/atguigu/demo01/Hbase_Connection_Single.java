package com.atguigu.demo01;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

class HBase_Connection_Single {
    private static Connection connection=null;

    public static Connection getConnection(){
        try {
            if (connection == null) {
                Configuration conf = new Configuration();
                conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
                connection = ConnectionFactory.createConnection(conf);

            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return connection;
    }

    public static void clossConnection(){
        try{
            if(connection!=null){
                connection.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        Connection connection = HBase_Connection_Single.getConnection();
        System.out.println(connection);
        System.out.println(HBase_Connection_Single.getConnection());
        HBase_Connection_Single.clossConnection();
    }

}
