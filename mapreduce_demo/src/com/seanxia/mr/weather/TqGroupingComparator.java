package com.seanxia.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TqGroupingComparator extends WritableComparator{

	public TqGroupingComparator() {
		super(TQ.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TQ t1 = (TQ)a;
		TQ t2 = (TQ)b;
		
		// 聚合只需要年月
		int y = Integer.compare(t1.getYear(), t2.getYear());
		if (y==0) {
			return Integer.compare(t1.getMonth(), t2.getMonth());
		}
		return y;
	}
	
	
	
	
}
