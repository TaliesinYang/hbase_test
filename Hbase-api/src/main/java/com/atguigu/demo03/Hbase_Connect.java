package com.atguigu.demo03;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class Hbase_Connect {
    public static void main(String[] args) throws IOException {
        //1.创建配置文件对象
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //2.使用工厂类创建链接对象
        Connection connection = ConnectionFactory.createConnection(conf);
        System.out.println(connection);
        //3.关闭链接
        connection.close();
    }
}
