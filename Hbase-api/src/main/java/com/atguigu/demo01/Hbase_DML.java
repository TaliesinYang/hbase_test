package com.atguigu.demo01;

import com.atguigu.demo02.Hbase_Connection_Single;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

public class Hbase_DML {
    private static Connection connection = null;

    static {
        getConnection();
    }

    public static Connection getConnection() {
        if (connection == null) {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            try {
                connection = ConnectionFactory.createConnection(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return connection;
    }

    public static void closeConnection() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void pullCell(String namespace, String tableName, String rowKey, String fammily, String cloumn, String value) {
        //1.获取Put对象
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Put put = null;
        //2.获取put对象
        put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(fammily), Bytes.toBytes(cloumn), Bytes.toBytes(value));
        //调用put写入数据
        try {
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
        //关闭table对象
        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static void getCell(String namespace, String tableName, String rowKey, String fammily, String cloumn) throws IOException {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Get get = new Get(Bytes.toBytes(rowKey));
        get.readAllVersions();
        get.addColumn(Bytes.toBytes(fammily),Bytes.toBytes(cloumn));
        Result result = table.get(get);
        for (Cell cell : result.rawCells()) {
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + "-" + Bytes.toString(CellUtil.cloneFamily(cell)) + "-" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "-" + Bytes.toString(CellUtil.cloneValue(cell)));
        }
        table.close();
    }

    public static void delectCells(String namespace, String tableName, String rowKey, String fammily, String cloumn) throws IOException {
        Table table = connection.getTable(TableName.valueOf(namespace, tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        delete.addColumn(Bytes.toBytes(fammily),Bytes.toBytes(cloumn));
        table.delete(delete);
        table.close();
    }

    public static void scanTable(String namespace, String tableName, String startRow, String stopRow) throws IOException {
        //1.获取表对象
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(namespace, tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        //2.
        ResultScanner rs = null;
        Scan scan = new Scan();
        scan.readAllVersions();
        scan.withStartRow(Bytes.toBytes(startRow));
        scan.withStopRow(Bytes.toBytes(stopRow), true);

        try {
            rs = table.getScanner(scan);
        } catch (IOException e) {
            e.printStackTrace();
        }
        for (Result result : rs) {
            for (Cell cell : result.rawCells()) {
                System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + "-" + Bytes.toString(CellUtil.cloneFamily(cell)) + "-" + Bytes.toString(CellUtil.cloneQualifier(cell)) + "-" + Bytes.toString(CellUtil.cloneValue(cell)));
            }
        }
        table.close();
    }

    public static void main(String[] args) throws IOException {

//        Hbase_DML.pullCell("default", "student", "1003", "info", "name", "zhuzhu");
//        Hbase_DML.pullCell("default", "student", "1003", "info", "name", "zhuzhu");
//        Hbase_DML.pullCell("default", "student", "1003", "info", "name", "zhuzhu");
//        Hbase_DML.pullCell("default", "student", "1003", "info", "name", "zhuzhu");

        delectCells("default", "student", "1001", "info", "age");
        Hbase_DML.scanTable("default","student","1001","1009");
//        getCell("default", "student", "1003", "info", "name");
        Hbase_DML.closeConnection();

    }

}
