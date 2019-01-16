package com.seanxia.mr.friend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class Map02 extends Mapper<LongWritable, Text, FriendSort, IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		// 拿到Reduce01中的每行数据
		String lines = value.toString();
		
		// 每行数据按照空格切分分别设置给friend01、friend02和hot
		String friend01 = StringUtils.split(lines, ' ')[0];
		String friend02 = StringUtils.split(lines, ' ')[1];
		int hot = Integer.parseInt(StringUtils.split(lines, ' ')[2]);
		
		System.out.println(friend01 + ' ' + friend02 + ' ' + hot);
		System.out.println(friend02 + ' ' + friend01 + ' ' + hot);
		// 按照自定义的规则 FriendSort 来排序，因为 FriendSort 中相等的 Key 是按照hot来排序的，所以Key中要带上hot值
		context.write(new FriendSort(friend01, friend02, hot), new IntWritable(hot));
		context.write(new FriendSort(friend02, friend01, hot), new IntWritable(hot));
		// cat hadoop 2 2
		// cat hello 2 2
	}
}








