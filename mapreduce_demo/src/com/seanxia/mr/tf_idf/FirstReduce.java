package com.seanxia.mr.tf_idf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FirstReduce extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	@Override
	protected void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable i : value) {
			sum += i.get();
		}
		if (key.equals(new Text("count"))) {
			System.out.println(key.toString() + "==========" + sum);
		}
		context.write(key, new IntWritable(sum));
	}
	
	
}
