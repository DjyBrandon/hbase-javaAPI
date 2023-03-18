package main;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import utils.initLogRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;

public class put2HBase {
		// 连接HBase数据库，插入数据到钞票表并查询批量插入数据和查询操作
		public static void main(String[] args) throws IOException, ParseException {
			initLogRecord.initLog();
			String tableName = "identify_rmb_records";
			String line;
			Configuration conf = HBaseConfiguration.create();
			conf.set("hbase.master", "node:16000");  // 指定HMaster
			conf.set("hbase.rootdir", "hdfs://node:8020/hbase");  // 指定HBase在hdfs上的储存路径
			conf.set("hbase.zookeeper.quorum", "node");  // 指定使用的zookeeper集群
			conf.set("hbase.zookeeper.property.clientPort", "2181");  // 指定zookeeper端口
			Connection conn = ConnectionFactory.createConnection(conf);	  // 获取连接
			Table table = conn.getTable(TableName.valueOf(tableName));

			SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm");
			// 按行读取数据
			BufferedReader br = new BufferedReader(new FileReader("src/resources/stumer_in_out_details.txt"));
			while ((line = br.readLine())!=null) {
				String[] lines = line.split(",");
				Put put = new Put(lines[0].getBytes());
				long timeStamp = format.parse(lines[2]).getTime(); 
				// 插入各个cell对应的具体值
				put.addColumn("op_www".getBytes(),"exist".getBytes(),timeStamp, lines[1].getBytes());
				put.addColumn("op_www".getBytes(),"Bank".getBytes(),timeStamp, lines[3].getBytes());
				if (lines.length == 4) {
					put.addColumn("op_www".getBytes(),"uId".getBytes(),timeStamp, "".getBytes());
				} else {
					put.addColumn("op_www".getBytes(),"uId".getBytes(),timeStamp, lines[4].getBytes());
				}
				table.put(put);
			}
			br.close();
		}
}