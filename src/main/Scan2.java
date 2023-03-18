package main;

import java.io.IOException;
import java.util.Iterator;

import utils.initLogRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;

public class Scan2 {
	public static void main(String[] args) throws IOException {
		initLogRecord.initLog();
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.master", "node:16000");//指定HMaster
		conf.set("hbase.rootdir", "hdfs://node:8020/hbase");//指定HBase在hdfs上的储存路径
		conf.set("hbase.zookeeper.quorum", "node");//指定使用的zookeeper集群
		conf.set("hbase.zookeeper.property.clientPort", "2181");//指定zookeeper端口
		Connection conn = ConnectionFactory.createConnection(conf);	//获取连接
		Table table = conn.getTable(TableName.valueOf("identify_rmb_records"));
		Scan scan = new Scan();
		scan.addFamily("op_www".getBytes());
		scan.setMaxVersions();  // 获取多个版本
		scan.setStartRow("AABX0673".getBytes());
		scan.setMaxVersions(2);

		ResultScanner scanner = table.getScanner(scan);
		Iterator<Result> reslut = scanner.iterator();
		while (reslut.hasNext()) {
			Result re = reslut.next();
			for (Cell cell:re.rawCells()) {  // 遍历Result中的数据
				System.out.println();
				System.out.print(new String(CellUtil.cloneRow(cell))+"|");  // 打印rowkey
				System.out.print(new String(CellUtil.cloneFamily(cell))+"|");  // 打印列簇
				System.out.print(new String(CellUtil.cloneQualifier(cell))+"|");	 // 打印列名
				System.out.print(new String(CellUtil.cloneValue(cell)));  // 打印具体值
			}
		}
	}
}