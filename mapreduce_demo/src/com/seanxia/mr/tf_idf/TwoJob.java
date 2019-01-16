package com.seanxia.mr.tf_idf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 第二个Job：某个单词出现过的文章数DF
 * 这里用于求IDF（逆向文件频率 ）IDF = log(微博总数N/（DF+1）)
 * @author Xss
 *
 */
public class TwoJob {

	public static void main(String[] args) throws IOException {
		// conf
		Configuration conf =new Configuration();
		conf.set("fs.defaultFS", "hdfs://sean01:8020");
		conf.set("yarn.resourcemanager.hostname", "sean03:8088");
		
		// job
		Job job =Job.getInstance(conf);
		job.setJarByClass(TwoJob.class);
		job.setJobName("weibo2");
		
		// Map
		job.setMapperClass(TwoMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Reduce
		job.setReducerClass(TwoReduce.class);
//		job.setCombinerClass(TwoReduce.class);
		
		// input
		FileInputFormat.addInputPath(job, new Path("/TF-IDF/output/weibo1"));
		
		// output
		Path output = new Path("/TF-IDF/output/weibo2");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		// 写出
		FileOutputFormat.setOutputPath(job, output);
		
		// 打印结果
		try {
			// 等待任务完成
			boolean f = job.waitForCompletion(true);
			if (f) {
				System.out.println("two job success ...");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
