package com.atguigu.demo02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class Hbase_DML {
    private static Connection connection=null;
    static {
        getConnection();
    }

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
    public static void putCell(String namespace,String tableName,String rowKey,String family,String columnName,String value) throws IOException {
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));

        //2.获取Put对象
        Put put = new Put(getBytes(rowKey));
        put.addColumn(getBytes(family),getBytes(columnName),getBytes(value));
        //3.table执行put操作
        table.put(put);
        //4.关闭table
        table.close();

    }
    public static void getCell(String namespace,String tableName,String rowKey,String family,String columnName) throws IOException {
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        //2.获取get对象
        Get get = new Get(getBytes(rowKey));
        get.addColumn(getBytes(family),getBytes(columnName));
        //3.执行get操作,获取到result对象
        Result result = table.get(get);
        //4.解析result
        for (Cell cell : result.rawCells()) {
            String rsString=Bytes.toString(CellUtil.cloneRow(cell))+"-"+Bytes.toString(CellUtil.cloneFamily(cell))+"-"+Bytes.toString(CellUtil.cloneQualifier(cell))+"-"+Bytes.toString(CellUtil.cloneValue(cell));
            System.out.println(rsString);
        }
        //关闭table
        table.close();
    }

    public static void delectCells(String namespace,String tableName,String rowKey,String family,String columnName) throws IOException {
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        //2.创建delete对象
        Delete delete = new Delete(getBytes(rowKey));
        delete.addColumn(getBytes(family),getBytes(columnName));
        //3.执行delete操作
        table.delete(delete);
        //4.关闭table
        table.close();
    }
    public static void scanTable(String spacename,String tableName,String startRow,String stopRow) throws IOException {
        //1.获取表对象
        Table table = connection.getTable(TableName.valueOf(spacename, tableName));

        //2.获取scan对象
        Scan scan = new Scan();
        scan.withStartRow(getBytes(startRow));
        scan.withStopRow(getBytes(stopRow),true);
        //3.执行scan操作,并解析打印
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                String rsString=Bytes.toString(CellUtil.cloneRow(cell))+"-"+Bytes.toString(CellUtil.cloneFamily(cell))+"-"+Bytes.toString(CellUtil.cloneQualifier(cell))+"-"+Bytes.toString(CellUtil.cloneValue(cell));
                System.out.println(rsString);
            }
        }
        //4.关闭表
        table.close();
    }
    private static byte[] getBytes(String value){
        return Bytes.toBytes(value);
    }


}
