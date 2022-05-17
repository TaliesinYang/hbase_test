package com.atguigu.demo01;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

public class hbaseconnect {
    public static void main(String[] args) throws IOException {
        //0.创建配置对象,设置链接属性
        Configuration conf = new Configuration();
        conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
        //1.通过链接工厂获取链接
        Connection connection = ConnectionFactory.createConnection(conf);
        System.out.println(connection);

        //关闭链接
        connection.close();
    }

}
