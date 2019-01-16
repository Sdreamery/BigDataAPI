package com.seanxia.mr.tf_idf;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

/**
 * 第一个MR自定义分区
 * @author Xss
 */
public class FirstPartition extends HashPartitioner<Text, IntWritable>{

	@Override
	public int getPartition(Text key, IntWritable value, int reduceCount) {
		// 把 Key为 count（统计微博的数量）的放入第四个分区
		if (key.equals(new Text("count"))) { 
			return 3;
		} else {
			// 除掉 count 这个分区，数据放到剩下的3个分区
			return super.getPartition(key, value, reduceCount-1);
		}
	}
	
}
