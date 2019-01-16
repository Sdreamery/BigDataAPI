package com.seanxia.mr.tf_idf;

import java.io.IOException;
import java.io.StringReader;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

/**
 * 第一个MR，计算TF词频(某个词出现的次数/微博文章总数)
 * @author Xss
 */
public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		//3823890210294392	今天我约了豆浆，油条...
		String[] v = value.toString().trim().split("\t");
		
		if (v.length >=2) {
			String id = v[0].trim(); //文章ID
			String content = v[1].trim(); //文章内容
			
			// 流读取（因为要分词不能直接读入）
			StringReader sr = new StringReader(content);
			
			// 把字符串放进 IK中文分词器 
			IKSegmenter ikSegmenter = new IKSegmenter(sr, true); // true使用自动分词
			
			// 拿到每一个分词
			Lexeme word = null;
			// 循环每条内容进行分词
			while ((word = ikSegmenter.next()) != null ) { 
				String w = word.getLexemeText();
				// 逐个词写出（一个词 + 一个ID）
				context.write(new Text(w + "_" + id), new IntWritable(1));
				//今天_3823890210294392		1
				//豆浆_3823890210294392		1
				//豆浆机_3823890210294392		1
				//豆浆机_3823890210294392		1
			}
			// 每出来一条文章，记一下数
			context.write(new Text("count"), new IntWritable(1));
			
		} else {
			System.out.println(value.toString() + "----------------------");
		}
	}
	
}







