package main;

import java.io.IOException;

import utils.initLogRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.util.Bytes;

public class createHBaseTable {

	// 连接HBase，并创建钞票表
	public static void main(String[] args) throws IOException {
		initLogRecord.initLog();
		String tableName = "identify_rmb_records";
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "node:16000");  // 指定HMaster
		conf.set("hbase.rootdir", "hdfs://node:8020/hbase");  // 指定HBase在hdfs上的储存路径
		conf.set("hbase.zookeeper.quorum", "node");  // 指定使用的zookeeper集群
		conf.set("hbase.zookeeper.property.clientPort", "2181");  // 指定zookeeper端口
		Connection conn = ConnectionFactory.createConnection(conf);  // 获取连接
		Admin admin = conn.getAdmin();  // 创建Admin实例
		TableName tablename = TableName.valueOf(tableName);  // 创建表名
		HTableDescriptor ht = new HTableDescriptor(tablename);  // 用表名创建HTableDescriptor对象
		byte[][] Regions = new byte[][] {
				Bytes.toBytes("AAAR3333"),
				Bytes.toBytes("AABI6666")
		};
		ht.addFamily(new HColumnDescriptor("op_www").setMaxVersions(1000));  // 设置列簇名和最大版本数
		admin.createTable(ht,Regions);  // 创建数据表
	}

}