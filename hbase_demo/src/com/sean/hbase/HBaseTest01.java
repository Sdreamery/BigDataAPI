package com.sean.hbase;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseTest01 {
	static String TN = null;
	static Configuration config = null;
	static HBaseAdmin hBaseAdmin = null;
	
	public static void main(String[] args) {
		// 表名
		TN = "sean";
		config = new Configuration();
		// Zookeeper的集群地址
		config.set("hbase.zookeeper.quorum", "sean01,sean02,sean03");
		try {
			hBaseAdmin = new HBaseAdmin(config);
			//创建表
//			createHtable();
			
			//添加记录
//			putData();
//			putData2();
			
			//查看全表记录
//			scanData();
			//scan也可以进行过滤查询
//			scanData2();
			
			//查看某一行的记录
//			getData();
			
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	private static void getData() throws IOException {
		HTable hTable =new HTable(config, TN);
		// 对rowKey进行过滤
		Get get = new Get("rk_001".getBytes());
		// 对name进行过滤
		get.addColumn("cf1".getBytes(), "name".getBytes());
		Result result = hTable.get(get);
//		System.out.println(result);
		for (Cell cell : result.rawCells()) {
			System.out.println(
					"rowKey: "+Bytes.toString(result.getRow())+"\t"+
					"qualifier: "+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"+
					"values: "+Bytes.toString(CellUtil.cloneValue(cell))
					);
		}
		
	}

	private static void scanData() throws IOException {
		HTable hTable = new HTable(config, TN);
		Scan scan = new Scan();
		ResultScanner scanner = hTable.getScanner(scan);
		for (Result result : scanner) {
			for (Cell cell : result.rawCells()) {
				System.out.println(
						"rowKey: "+Bytes.toString(result.getRow())+"\t"+
						"qualifier: "+Bytes.toString(CellUtil.cloneQualifier(cell))+"\t"+
						"values: "+Bytes.toString(CellUtil.cloneValue(cell))
						);
			}
		}
	}
	
	private static void scanData2() throws IOException {
		HTable hTable = new HTable(config, TN);
		
		Scan scan = new Scan();
		// 对rowKey进行过滤限制，左闭右开，不包括rk_002
		scan.setStartRow("rk_001".getBytes());	//扫描起始位置
		scan.setStopRow("rk_002".getBytes());	//扫描结束位置
		// 对name进行过滤
		scan.addColumn("cf1".getBytes(), "name".getBytes());
		
		ResultScanner scanner = hTable.getScanner(scan);
		for (Result result : scanner) {
			System.out.println("rowKey: "+Bytes.toString(result.getRow()));
			for (Cell cell : result.rawCells()) {
				System.out.println(
						"\t"+Bytes.toString(CellUtil.cloneQualifier(cell))+
						"\t"+Bytes.toString(CellUtil.cloneValue(cell))
						);
			}
		}
		
		
	}
	

	private static void putData() throws IOException {
		HTable hTable = new HTable(config, TN);
		Put put = new Put("rk_001".getBytes());
		put.add("cf1".getBytes(), "name".getBytes(), "xiafang".getBytes());
		put.add("cf1".getBytes(), "age".getBytes(), "20".getBytes());
		put.add("cf1".getBytes(), "addr".getBytes(), "shanghai".getBytes());
		hTable.put(put);
		System.out.println("---------添加记录完成-----------");
		
	}
	
	private static void putData2() throws IOException {
		HTable hTable = new HTable(config, TN);
		Put put = new Put("rk_002".getBytes());
		put.add("cf1".getBytes(), "name".getBytes(), "ali".getBytes());
		put.add("cf1".getBytes(), "age".getBytes(), "18".getBytes());
		put.add("cf1".getBytes(), "addr".getBytes(), "beijing".getBytes());
		hTable.put(put);
		System.out.println("---------添加记录完成-----------");
		
	}

	private static void createHtable() throws IOException {
		if (hBaseAdmin.tableExists(TN)) {
			// 表存在先屏蔽掉再删除
			hBaseAdmin.disableTable(TN);
			hBaseAdmin.deleteTable(TN);
			System.out.println("-----------表存在，已删除-------");
		}
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));
		HColumnDescriptor family = new HColumnDescriptor("cf1");
		family.setMaxVersions(1);	//保留的版本
		family.setInMemory(true);   //是否加载到内存
		family.setBlockCacheEnabled(true);	//是否开启BlockCache
//		family.setTimeToLive(1*24*60*60);	//设置数据的有效期
		desc.addFamily(family);
		hBaseAdmin.createTable(desc);
		System.out.println("------------表创建完成---------------");
		
	}
	
	
}







