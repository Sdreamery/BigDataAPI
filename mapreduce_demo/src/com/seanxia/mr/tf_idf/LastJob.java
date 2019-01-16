package com.seanxia.mr.tf_idf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * 第三个任务：tf-idf计算并输出结果
 *	注意：这里因为数据放在缓存，不能直接运行，需要导jar包放到服务器中去运行
 * @author Xss
 */
public class LastJob {
	
	public static void main(String[] args) throws IOException {
		Configuration conf =new Configuration();		
//		conf.set("fs.defaultFS", "hdfs://node01:8020");
//		conf.set("yarn.resourcemanager.hostname", "node02:8088");
		
		// job
		Job job =Job.getInstance(conf);
		job.setJarByClass(LastJob.class);
		job.setJobName("weibo3");
		
		/**
		 * IDF = log(微博总数N/（DF+1）)
		 * 这里放到缓存中是因为数据无需处理，可以直接拿来用
		 */
		// 把微博总数N加载到 Cache 缓存中
		job.addCacheFile(new Path("/TF-IDF/output/weibo1/part-r-00003").toUri());
		// 把DF加载到 Cache 缓存中（该词在多少篇微博出现过）===》可以求词频（IDF）
		job.addCacheFile(new Path("/TF-IDF/output/weibo2/part-r-00000").toUri());
		
		// 设置map任务的输出key类型、value类型
		job.setMapperClass(LastMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		// 设置Reduce任务
		job.setReducerClass(LastReduce.class);
		
		// input
		FileInputFormat.addInputPath(job, new Path("/TF-IDF/output/weibo1"));
		
		// output
		Path outpath =new Path("/TF-IDF/output/weibo3");
		FileSystem fs =FileSystem.get(conf);
		if(fs.exists(outpath)){
			fs.delete(outpath, true);
		}
		// 写出
		FileOutputFormat.setOutputPath(job,outpath );
		
		// 打印结果	
		boolean f;
		try {
			// 等待任务完成
			f = job.waitForCompletion(true);
			if(f){
				System.out.println("执行job成功");
			}
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
			
	}
}
