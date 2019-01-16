package com.seanxia.mr.friend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce02 extends Reducer<FriendSort, IntWritable, Text, NullWritable>{
	
	@Override
	protected void reduce(FriendSort friend, Iterable<IntWritable> hot, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		for (IntWritable i : hot) {
			sum += i.get();
		}
		System.out.println("=========================");
		String msg = friend.getFriend01() + ' ' + friend.getFriend02() + ' ' + sum;
		System.out.println(msg);
		context.write(new Text(msg), NullWritable.get());
		
	}
	
}
