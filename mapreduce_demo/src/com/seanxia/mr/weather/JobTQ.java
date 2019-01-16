package com.seanxia.mr.weather;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class JobTQ {
	public static void main(String[] args) throws IOException {
		// 环境配置
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://sean01:8020");
		conf.set("yarn.resourcemanager.hostname", "node03:8088");
		
		// job
		Job job = Job.getInstance(conf);
		job.setJarByClass(JobTQ.class);
		job.setJobName("TQ");
		
		// Map
		job.setMapperClass(TqMapper.class);
		job.setMapOutputKeyClass(TQ.class);
		job.setMapOutputValueClass(Text.class);
		
		// Reduce
		job.setReducerClass(TqReducer.class);
		job.setNumReduceTasks(3); //3个Reduce结果
		
		// input
		FileInputFormat.addInputPaths(job, "/TQ/input/tq");
		
		// output
		Path output = new Path("/TQ/output");
		FileSystem fs = FileSystem.get(conf);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		// 写出到Reduce
		FileOutputFormat.setOutputPath(job, output);
		
		// 磁盘分区
		job.setPartitionerClass(TqPartitioner.class);
		
		// Map阶段的聚合，将相同月份的聚合在一起
		job.setCombinerKeyGroupingComparatorClass(TqGroupingComparator.class);
		
		// 二次排序 -- 对温度倒序排序
		job.setSortComparatorClass(TqSortComparator.class);
		job.setGroupingComparatorClass(TqGroupingComparator.class);
	
		// 打印结果
		boolean f;
		try {
			// 等待任务是否成功完成
			f = job.waitForCompletion(true);
			if (f) {
				System.out.println("success ...");
			}else{
				System.out.println("------------------");
			}
			
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	
	}
	
	
}







