package com.seanxia.mr.friend;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class JobFriends {
	public static void main(String[] args) throws IOException {
		Boolean flag = jobOne();
		if (flag) {
			jobTwo();
		}
	}

	private static Boolean jobOne() throws IOException {
		// 环境变量
		Configuration conf =new Configuration();
		conf.set("fs.defaultFS", "hdfs://sean01:8020");
		conf.set("yarn.resourcemanager.hostname", "sean03:8088");
		
		/**
		 * 设置job任务的相关信息
		 */
		Boolean flag = false;
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(JobFriends.class);
		job.setJobName("fof one job");
		
		// 设置任务类MapTask和ReduceTask
		job.setMapperClass(Map01.class);
		job.setReducerClass(Reduce01.class);
		
		// 设置输出的Key和Value类型
		job.setMapOutputKeyClass(FoF.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 指定输入的数据文件（读取HDFS上的文件）
		FileInputFormat.addInputPath(job, new Path("/QQ/input/qq.txt"));
		
		// 指定输出结果的路径
		Path output = new Path("/QQ/output/01");
		
		FileSystem fs = FileSystem.get(conf);
		// 路径存在就删除
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		
		//  写出数据
		FileOutputFormat.setOutputPath(job, output);
		
		// 打印结果
		try {
			// 等待任务是否成功完成
			flag = job.waitForCompletion(true);
			if (flag) {
				System.out.println("job01 success ...");
			} 
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// 根据返回的Boolean判断是否执行JobTwo
		return flag;
	}
	
	
	static Boolean jobTwo() throws IOException {
		// 环境变量
		Configuration config = new Configuration();
		config.set("fs.defaultFS", "hdfs://sean01:8020");
		config.set("yarn.resourcemanager.hostname", "sean03:8088");
		
		/**
		 * 设置job任务的相关信息
		 */
		boolean flag = false;
		Job job = Job.getInstance(config);
		
		job.setJarByClass(JobFriends.class);
		job.setJobName("fof two job");
		
		// 设置Map和Reduce的任务类
		job.setMapperClass(Map02.class);
		job.setReducerClass(Reduce02.class);
		
		// 设置输出的Key和Value类型
		job.setMapOutputKeyClass(FriendSort.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 指定输入文件的来源
		FileInputFormat.addInputPath(job, new Path("/QQ/output/01"));
		
		// 指定 输出结果的路径
		Path output = new Path("/QQ/output/02");
		
		FileSystem fs = FileSystem.get(config);
		if (fs.exists(output)) {
			fs.delete(output, true);
		}
		
		// 写出
		FileOutputFormat.setOutputPath(job, output);
		
		// 结果打印输出
		try {
			// 等待任务是否成功完成
			flag = job.waitForCompletion(true);
			if (flag) {
				System.out.println("job 2 success ...");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		return flag;
	}



	
	
}









