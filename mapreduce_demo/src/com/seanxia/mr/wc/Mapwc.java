package com.seanxia.mr.wc;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class Mapwc extends Mapper<LongWritable, Text, Text, IntWritable> {

	@Override
	protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String line = value.toString();
		
		// 推荐这种方法（包括制表符和换行等等）
		StringTokenizer words = new StringTokenizer(line);
		while (words.hasMoreTokens()) {
			context.write(new Text(words.nextToken()), new IntWritable(1));
		}
		
//		String words[] = StringUtils.split(line, ' ');
//		for (String ww : words) {
//			context.write(new Text(ww), new IntWritable(1));
//		}
		
	}
	
}
