package com.seanxia.mr.weather;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class TQ implements WritableComparable<TQ>{
	private int year;
	private int month;
	private int day;
	private int wd;
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getWd() {
		return wd;
	}

	public void setWd(int wd) {
		this.wd = wd;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(wd);
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year = in.readInt();
		this.month = in.readInt();
		this.day = in.readInt();
		this.wd = in.readInt();
	}

	@Override
	public int compareTo(TQ that) {
		// 先比较年份
		int y = Integer.compare(this.getYear(), that.getYear());
		if (y==0) {
			// 如果年份相同，就比较月份
			int m = Integer.compare(this.getMonth(), that.getMonth());
			if (m==0) {
				// 如果年份相同，就返回天
				return Integer.compare(this.getDay(), that.getDay());
			}
			return m;
		}
		return y;
	}
	
}
