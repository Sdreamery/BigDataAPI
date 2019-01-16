package com.seanxia.mr.wc;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducewc extends Reducer<Text, IntWritable, Text, IntWritable> {
	
	@Override
	protected void reduce(Text words, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
//      (The,{1,1,1,1})  
//      (Hadoop,{1,1})
		int sum = 0;
		for (IntWritable n : values) {
			sum+=n.get();
		}
		context.write(words, new IntWritable(sum));
	}
	
}
