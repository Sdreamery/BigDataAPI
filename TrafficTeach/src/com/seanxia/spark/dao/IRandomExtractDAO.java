package com.seanxia.spark.dao;

import java.util.List;

import com.seanxia.spark.domain.RandomExtractCar;
import com.seanxia.spark.domain.RandomExtractMonitorDetail;

/**
 * 随机抽取car信息管理DAO类
 * @author root
 *
 */
public interface IRandomExtractDAO {
	void insertBatchRandomExtractCar(List<RandomExtractCar> carRandomExtracts);
	
	void insertBatchRandomExtractDetails(List<RandomExtractMonitorDetail> r);
	
}
