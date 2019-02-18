package com.seanxia.java.sql.dataframe.serializeTest.bean;

import java.io.Serializable;


public class Fruit implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	public String common ;
	public String name ;
	public String getCommon() {
		return common;
	}
	public void setCommon(String common) {
		this.common = common;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	
}
