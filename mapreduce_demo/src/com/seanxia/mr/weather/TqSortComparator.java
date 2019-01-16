package com.seanxia.mr.weather;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TqSortComparator extends WritableComparator{

	public TqSortComparator() {
		super(TQ.class, true);
	}

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		TQ t1 = (TQ)a;
		TQ t2 = (TQ)b;
		
		int y = Integer.compare(t1.getYear(), t2.getYear());
		if (y==0) {
			int m = Integer.compare(t1.getMonth(), t2.getMonth());
			if (m==0) {
				return -Integer.compare(t1.getDay(), t2.getDay());
			}
			return m;
		}
		
		return y;
	}

	
	
	
	
}
