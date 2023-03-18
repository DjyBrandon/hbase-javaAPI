package main;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class ImportMapper extends Mapper<LongWritable, Text, ImmutableBytesWritable, Put>{
	private Put put =null;
	private ImmutableBytesWritable rowkey = new ImmutableBytesWritable();
	SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm");
	@Override
	protected void map(LongWritable key, Text value,
			Mapper<LongWritable, Text, ImmutableBytesWritable, Put>.Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split(",",-1);
		
		rowkey.set(Bytes.toBytes(values[0]));
		put= new Put(values[0].getBytes());
		long timeStamp = 0;
		try {
			timeStamp = format.parse(values[2]).getTime();
		} catch (ParseException e) {
			e.printStackTrace();
		} 
		put.addColumn("op_www".getBytes(),"exist".getBytes(),timeStamp, values[1].getBytes());
		put.addColumn("op_www".getBytes(),"Bank".getBytes(),timeStamp, values[3].getBytes());
		put.addColumn("op_www".getBytes(),"uId".getBytes(),timeStamp, values[4].getBytes());
		context.write(rowkey, put);
	}
}