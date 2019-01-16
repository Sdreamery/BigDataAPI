package com.seanxia.mr.friend;

import org.apache.hadoop.io.Text;

public class FoF extends Text{

	private String friend02;

	public FoF() {
		super();
	}
	
	// 带参构造
	public FoF(String friend01, String friend02) {
		set(getof(friend01, friend02));
	}
	
	// 自定义排序方法
	private String getof(String friend01, String friend02) {
		int c = friend01.compareTo(friend02);
		if (c > 0) {
			return friend02 + '\t' + friend01;
		}
		return friend01 + '\t' + friend02;
	}
	
}
