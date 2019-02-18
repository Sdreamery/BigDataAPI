package com.sxt.java.sql.dataframe.serializeTest.bean;

import java.io.Serializable;

public class Apple extends Fruit  {
	
	
//	private static final long serialVersionUID = 1L;
	public String name ;
    public String color;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}

	public String getColor() {
		return color;
	}
	public void setColor(String color) {
		this.color = color;
	}

}
