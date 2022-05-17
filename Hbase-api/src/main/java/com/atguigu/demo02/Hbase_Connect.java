package com.atguigu.demo02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class Hbase_Connect {
    public static void main(String[] args) throws IOException {
        //1.创建配置文件,并添加配置
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //2.通过工厂类获取到链接
        Connection connection = ConnectionFactory.createConnection();
        System.out.println(connection);

        //3.关闭链接
        connection.close();
    }
}
