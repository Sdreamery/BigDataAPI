package com.seanxia.mr.friend;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.util.StringUtils;

public class Reduce01 extends Reducer<FoF, IntWritable, Text, NullWritable>{
	
	@Override
	protected void reduce(FoF key, Iterable<IntWritable> values, Context context) 
			throws IOException, InterruptedException {
		// 设置计数统计每对二度关系的次数(统计标记为1的二度好友)
		int sum = 0;
		boolean flag = true;
		// 循环Map输入进来的value值（0或1）
		for (IntWritable i : values) {
			if (i.get() == 0) { //标记为0的一度好友去除
				flag = false;
				break;
			}
			sum += i.get();
		}
		
		// 拿到所有的二度好友进行处理
		if (flag) {
			// 把这些key按照 '\t' 切分放入数组中
			String str[] = StringUtils.split(key.toString(), '\t');
			// 分别按照空格隔开的格式写出二度关系并计次数 ===> cat hadoop 3
			String msg = str[0] + ' ' + str[1] + ' ' + sum;
			context.write(new Text(msg), NullWritable.get()); //这里因为Key里已经那拿到了所有的二度关系，Value不需要
		}
		
	}
	
}




