package com.seanxia.spark.dao.factory;

import com.seanxia.spark.dao.impl.AreaDaoImpl;
import com.seanxia.spark.dao.impl.CarTrackDAOImpl;
import com.seanxia.spark.dao.IAreaDao;
import com.seanxia.spark.dao.ICarTrackDAO;
import com.seanxia.spark.dao.IMonitorDAO;
import com.seanxia.spark.dao.IRandomExtractDAO;
import com.seanxia.spark.dao.ITaskDAO;
import com.seanxia.spark.dao.IWithTheCarDAO;
import com.seanxia.spark.dao.impl.MonitorDAOImpl;
import com.seanxia.spark.dao.impl.RandomExtractDAOImpl;
import com.seanxia.spark.dao.impl.TaskDAOImpl;
import com.seanxia.spark.dao.impl.WithTheCarDAOImpl;

/**
 * DAO工厂类
 * @author root
 *
 */
public class DAOFactory {
	
	
	public static ITaskDAO getTaskDAO(){
		return new TaskDAOImpl();
	}
	
	public static IMonitorDAO getMonitorDAO(){
		return new MonitorDAOImpl();
	}
	
	public static IRandomExtractDAO getRandomExtractDAO(){
		return new RandomExtractDAOImpl();
	}
	
	public static ICarTrackDAO getCarTrackDAO(){
		return new CarTrackDAOImpl();
	}
	
	public static IWithTheCarDAO getWithTheCarDAO(){
		return new WithTheCarDAOImpl();
	}

	public static IAreaDao getAreaDao() {
		return  new AreaDaoImpl();
		
	}
}
