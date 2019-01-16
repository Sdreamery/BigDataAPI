package com.seanxia.mr.friend;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class Map01 extends Mapper<LongWritable, Text, FoF, IntWritable>{

	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// 拿出每一行数据
		String lines = value.toString();
		
		// 按照  \t 切分这些字段，然后放入一个数组中
		String friends[] = StringUtils.split(lines, '\t');
		
		// 循环遍历每一行的字段（一度人脉）
		for (int i = 0; i < friends.length; i++) {
			String friend = friends[i];
			// 写出数据按照自定义的格式FoF
			context.write(new FoF(friends[0],friend),new IntWritable(0)); //标记0为一度好友
			
			// 在一度人脉的基础上遍历每一行的字段（二度人脉）
			for (int j = i+1; j < friends.length; j++) {
				String friend2 = friends[j];
				context.write(new FoF(friend, friend2), new IntWritable(1));//标记1为二度好友
			}
		}
		
//		StringTokenizer friends = new StringTokenizer(lines);
//		while (friends.hasMoreTokens()) {
//			context.write(new FoF(friends.nextToken()), new IntWritable(0));
//		}
	
	}
}
