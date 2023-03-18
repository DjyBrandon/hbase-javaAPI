package main;

import java.io.IOException;

import utils.initLogRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

public class ImportToHBase {
	public static void main(String[] args)throws IOException, ClassNotFoundException, InterruptedException  {
        initLogRecord.initLog();
        String TABLE="identify_rmb_records";  // 设置表名
        Path inputDir = new Path("src/resources/stumer_in_out_details.txt");
        Configuration conf = new Configuration();
        String jobName = "Import to "+ TABLE;
        Job job = Job.getInstance(conf, jobName);
        job.setJarByClass(ImportToHBase.class);  // 声明Driver类
        FileInputFormat.setInputPaths(job, inputDir);  // 添加输入路径
        job.setInputFormatClass(TextInputFormat.class);
        job.setMapperClass(ImportMapper.class);  // 设置Mapper类
        TableMapReduceUtil.initTableReducerJob(
        		TABLE,  // 连接的表明
        		null,  // 设置Reducer输出格式，因为没有Reduce，所以没有输出值
        		job);
        job.setNumReduceTasks(0);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}