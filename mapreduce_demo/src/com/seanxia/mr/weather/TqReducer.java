package com.seanxia.mr.weather;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TqReducer extends Reducer<TQ, Text, Text, Text>{
	
	Text rKey = new Text();
	Text rVal = new Text();
	
	@Override
	protected void reduce(TQ key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		int flag = 0;
		int day = 0;
		/**
		 * 先拉取拿到最初Map输出的数据，进行聚合
		 * 1949-10-01		34
		 * 1949-10-01       38
		 * 1949-10-02       36
		 * 1951-01-01       32
		 * ...
		 */
		// ============== 进入shuffle阶段（先分组聚合再按温度排序） =====================
		/**
		 * 开始聚合得到如下
		 * key: 1949-10-01		vals:{34,38}
		 * key: 1949-10-02      vals:{36}
		 * key: 1950-01-01      vals:{32}
		 * ...
		 */
//		==>shuffle--merge（合并 二次排序）==> 最高温度
//		此时温度经过二次排序（<年月,温度>）之后温度就是最高
		/**
		 * 在Reduce前得到如下数据，因为我们取的是同一个月里温度最高的前两天，所以天对我们来讲不重要
		 * 1949-10(-1)        38
		 * 1949-10(-2)        36
		 * 1949-10(-1)        34
		 * 10 月份取温度最高的前两天 就是 38和36
		 * 1950-1(-1)         32
		 * 1 月份取温度最高的前两天 就是 32 一天
		 * ...
		 */
		
		for (Text v : values) {
			// 第一天
			if (flag==0) {
				day = key.getDay();
				rKey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
				rVal.set(key.getWd()+"");
				System.out.println("*****"+key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
				System.out.println("#####"+key.getWd());
//				<年月,温度>
				context.write(rKey, rVal);
				flag ++;
			}
//			flg==1
			// 第二天
			if (flag!=0 && day!=key.getDay()) {
				rKey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
				rVal.set(key.getWd()+"");
				context.write(rKey, rVal);
				break;
			}
		}
		/**
		 * 最终得到的数据格式
		 * key: 1949-10-01		value:38
		 * key: 1949-10-02		value:36
		 * key: 1950-01-01      value:32
		 * ...
		 */
	}
	
}










