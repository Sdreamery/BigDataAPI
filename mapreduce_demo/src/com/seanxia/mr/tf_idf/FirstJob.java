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
 * 第一个Job拿到：1. 每个词出现的个数 A（某个词在某微博中出现的次数）  
 * 			   2. 某篇文章中所有词条的数目 B
 * 			   3.文章总数
 * 			词频 TF = (A/B)
 * @author Xss
 *
 */
public class FirstJob {
	public static void main(String[] args) throws IOException {
		// conf
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://sean01:8020");
		conf.set("yarn.resourcemanager.hostname", "sean03:8088");
		
		// job
		Job job = Job.getInstance(conf);
		job.setJarByClass(FirstJob.class);
		job.setJobName("weibo1");
		
		// Map
		job.setMapperClass(FirstMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// Reduce
		job.setReducerClass(FirstReduce.class);
		job.setNumReduceTasks(4); //4个Reduce结果
		
		// 分区
		job.setPartitionerClass(FirstPartition.class);
		
		// input
		FileInputFormat.addInputPath(job, new Path("/TF-IDF/input/weibo2.txt"));
		
		// output
		Path output = new Path("/TF-IDF/output/weibo1");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		// 写出
		FileOutputFormat.setOutputPath(job, output);
		
		// 打印结果
		boolean f;
		try {
			// 等待任务完成
			f = job.waitForCompletion(true);
			if (f) {
				System.out.println("first job success ...");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}








