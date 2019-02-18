package com.sxt.java.sql.dataframe.serializeTest.bean;

import java.io.Serializable;

public class User implements Serializable{  
	
    /**
	 * 
	 */
	private static final long serialVersionUID =1L;
	/**
	 * 
	 */
	private String username;
    private  String passwd;
//    private String passwd;

    public String getUsername() {
        return username;
    }  
      
    public void setUsername(String username) {
        this.username = username;
    }  
      
    public String getPasswd() {  
        return passwd;  
    }  
      
    public void setPasswd(String passwd) {  
        this.passwd = passwd;  
    }

	@Override
	public String toString() {
		return "User [username=" + username + ", passwd=" + passwd + "]";
	}  
  
	
}
