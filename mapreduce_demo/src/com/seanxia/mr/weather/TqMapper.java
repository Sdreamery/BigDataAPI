package com.seanxia.mr.weather;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class TqMapper extends Mapper<LongWritable, Text, TQ, Text>{
	TQ tq = new TQ();
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
//		1949-10-01 19:21:02		38c
//		1949-10-02 14:01:02		36c
//		1950-01-01 11:21:02		32c
		try {
			// 将每行数据Value按照制表符截取到数组strs中
			String[] strs = StringUtils.split(value.toString(), '\t');
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date date = null;
			// 拿到日期
			date = sdf.parse(strs[0]);
			
			Calendar cal = Calendar.getInstance();
			cal.setTime(date);
			
			tq.setYear(cal.get(Calendar.YEAR));
			tq.setMonth(cal.get(Calendar.MONTH)+1);
			tq.setDay(cal.get(Calendar.DAY_OF_MONTH));
			
			// 获取温度的整型，只截取前面数字部分
			int wd = Integer.parseInt(strs[1].substring(0, strs[1].length()-1));
			tq.setWd(wd);
			System.out.println("===>"+tq.toString()+' '+new Text().toString());
			
			context.write(tq, new Text());
//			key: 年月日   ， value: 温度
//			1949-10-01         34
//			1949-10-01         38
//			1949-10-01         37
//			1949-10-02         39
			
		} catch (ParseException e) {
			e.printStackTrace();
		}
		
	}
	
}










