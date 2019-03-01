package com.seanxia.spark.dao.impl;

import com.seanxia.spark.constant.Constants;
import com.seanxia.spark.jdbc.JDBCHelper;
import com.seanxia.spark.util.DateUtils;
import com.seanxia.spark.dao.IWithTheCarDAO;

public class WithTheCarDAOImpl implements IWithTheCarDAO {

	@Override
	public void updateTestData(String cars) {
		JDBCHelper jdbcHelper = JDBCHelper.getInstance();
		String sql = "UPDATE task set task_param = ? WHERE task_id = 3";
		Object[] params = new Object[]{"{\"startDate\":[\""+ DateUtils.getTodayDate()+"\"],\"endDate\":[\""+DateUtils.getTodayDate()+"\"],\""+ Constants.FIELD_CARS+"\":[\""+cars+"\"]}"};
		jdbcHelper.executeUpdate(sql, params);
	}

}
