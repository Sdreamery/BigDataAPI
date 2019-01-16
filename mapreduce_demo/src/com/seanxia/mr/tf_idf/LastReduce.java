package com.seanxia.mr.tf_idf;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LastReduce extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> iterable, Context context)
			throws IOException, InterruptedException {
		/**
		 * Reduce写出前进入Shuffle阶段
		 * 1. 先聚合（相同的Key做聚合）
		 * 3823930429533207        豆浆:0.96781	 豆浆机:0.11234
		 * 3823930429533200        豆浆:0.36781	 油条:0.11232
		 */
		StringBuffer sb = new StringBuffer();

		for (Text i : iterable) {
			sb.append(i.toString() + "\t");
		}

		context.write(key, new Text(sb.toString()));
	}

}
