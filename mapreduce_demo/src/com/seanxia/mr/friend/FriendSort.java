package com.seanxia.mr.friend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

import com.sun.swing.internal.plaf.basic.resources.basic;

public class FriendSort implements WritableComparable<FriendSort>{
	private String friend01;
	private String friend02;
	private int hot;

	public FriendSort() {
		super();
	}
	
	public FriendSort(String friend01, String friend02, int hot) {
		this.friend01 = friend01;
		this.friend02 = friend02;
		this.hot = hot;
	}

	public String getFriend01() {
		return friend01;
	}

	public void setFriend01(String friend01) {
		this.friend01 = friend01;
	}

	public String getFriend02() {
		return friend02;
	}

	public void setFriend02(String friend02) {
		this.friend02 = friend02;
	}

	public int getHot() {
		return hot;
	}

	public void setHot(int hot) {
		this.hot = hot;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.friend01 = in.readUTF();
		this.friend02 = in.readUTF();
		this.hot = in.readInt();
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeUTF(friend01);
		out.writeUTF(friend02);
		out.writeInt(hot);
	}

	@Override
	public int compareTo(FriendSort friend) {
		// cat hadoop 2 
		// cat hello 2 
		int f1 = friend01.compareTo(friend.getFriend01());
		int f2 = friend02.compareTo(friend.getFriend02());
		if (f1 == 0) {
			if (f2!=0) { //第二个不相等
				int h = Integer.valueOf(hot).compareTo(friend.getHot());
				if (h<0) {
					return h;
				}else if(h>0){
					return -h;
				}else{ //热度相等的时候
					if (f2<0) {
						return f2;
					}
				}
			}
			
		}
		return f1;
	}

}





