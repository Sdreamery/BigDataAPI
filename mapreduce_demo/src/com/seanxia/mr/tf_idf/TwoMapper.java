package com.seanxia.mr.tf_idf;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

//统计df：词在多少个微博中出现过
public class TwoMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		// 获取当前 mapper task的数据片段（split切片)
		FileSplit fs = (FileSplit) context.getInputSplit();
		
		// part-r-00003 是做统计的，要排除掉
		if (!fs.getPath().getName().contains("part-r-00003")) {

			//豆浆_3823890201582094	3
			String[] v = value.toString().trim().split("\t");
			if (v.length >= 2) {
				// 拿到前面词+ID的部分，根据"_"切分再拿到词
				String[] ss = v[0].split("_");
				if (ss.length >= 2) {
					String w = ss[0]; //拿到词
					// 写出这个词，并计数
					context.write(new Text(w), new IntWritable(1));
				}
			} else {
				System.out.println(value.toString() + "-------------");
			}
		}
	}
}






