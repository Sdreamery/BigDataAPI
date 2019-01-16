package com.seanxia.mr.wc;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Jobwc {
	public static void main(String[] args) throws IOException {
		// 1.环境变量
		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://sean01:8020");
		conf.set("yarn.resourcemanager.hostname", "sean03:8088");
		
		// 2.设置job任务的相关信息
		Job job = Job.getInstance(conf);
		job.setJarByClass(Jobwc.class);
		job.setJobName("wc");
		
		// 设置任务类MapTask和ReduceTask
		job.setMapperClass(Mapwc.class);
		job.setReducerClass(Reducewc.class);
		
		// 设置输出的Key和Value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		// 3.输入数据文件（读取HDFS上的文件）
		FileInputFormat.addInputPaths(job, "/wc/input/wc.txt");
		
		// 4.输出结果到指定地方（HDFS中已存在的路径不会自动覆盖）
		Path path = new Path("/wc/output");
		FileSystem fs = FileSystem.get(conf);
		// 如果存在就删除
		if (fs.exists(path)) {
			fs.delete(path,true);
		}
		// 写出数据
		FileOutputFormat.setOutputPath(job, path);
		
		// 5.结束，打印结果
		boolean f;
		try {
			// 等待任务是否成功完成
			f = job.waitForCompletion(true);
			if (f) {
				System.out.println("job success ...");
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
