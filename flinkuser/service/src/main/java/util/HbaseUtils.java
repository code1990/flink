package util;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * @program: flinkuser
 * @Date: 2019-10-27 14:13
 * @Author: code1990
 * @Description:
 */
public class HbaseUtils {
    private static Admin admin = null;
    private static Connection conn = null;

    static {
        //创建 hbase配置对象
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.rootdir", "hdfs://192.168.80.134:9000/hbase");
        //使用eclipse时必须添加这个，否则无法定位
        conf.set("hbase.zookeeper.quorum", "192.168.80.134");
        conf.set("hbase.client.scanner.timeout.period", "600000");
        conf.set("hbase.rpc.timeout", "600000");
        try {
            conn = ConnectionFactory.createConnection(conf);
            //得到管理程序
            admin = conn.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /*插入数据*/
    public static void put(String tableName, String rowKey, String familyName, Map<String, String> map) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));
        //把字符串转换为byte[]
        byte[] rowkeyByte = Bytes.toBytes(rowKey);
        Put put = new Put(rowkeyByte);
        if (map != null) {
            Set<Map.Entry<String, String>> set = map.entrySet();
            for (Map.Entry<String, String> entry : set) {
                String key = entry.getKey();
                Object value = entry.getValue();
                put.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(key), Bytes.toBytes(value + ""));
            }
        }
        table.put(put);
        table.close();
        System.out.println("ok");
    }

    public static String getData(String tableName, String rowKey, String familyName, String column) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));
        //将字符串转换为byte[]
        byte[] rowKeyByte = Bytes.toBytes(rowKey);
        Get get = new Get(rowKeyByte);
        Result result = table.get(get);
        byte[] resultBytes = result.getValue(familyName.getBytes(), column.getBytes());
        if (resultBytes == null) {
            return null;
        }
        return new String(resultBytes);
    }

    public static void putData(String tableName, String rowKey, String familyName, String column, String data) throws Exception {
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(rowKey.getBytes());
        put.addColumn(familyName.getBytes(), column.getBytes(), data.getBytes());
        table.put(put);
    }

}
