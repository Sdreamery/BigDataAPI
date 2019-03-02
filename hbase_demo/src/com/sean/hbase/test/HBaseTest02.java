package com.sean.hbase.test;

import java.io.IOException;
import java.net.URLDecoder;
import java.util.ArrayList;

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
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * rowkey设计优化
 * 创建 删除表
 * put （list）  一个数据文件都put表中
 * get （list）
 * scan  
 *     起-始row
 *     过滤器
 *         前缀
 *         列值（字符串比较）
 * @author Xss
 */
public class HBaseTest02 {
	static String TN = null;
	static Configuration config;
	static HBaseAdmin hBaseAdmin;

	public static void main(String[] args) {
		// 实例化配置参数
		TN = "sean";
		config = new Configuration();
		config.set("hbase.zookeeper.quorum", "sean01,sean02,sean03");

		try {
			hBaseAdmin = new HBaseAdmin(config);
			// 创建 删除表操作
//			createHbase();

			// 插入数据 put put<list>
//			putHbase();

			// scan获取数据
//			scanData();
//			System.out.println("scan获取数据完成");

			// get方式获取数据  get<list>
			getdata();
			System.out.println("get获取数据完成");
	  
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/*
	 * get方式获取数据
	 */
	private static void getdata() throws IOException {
		// 同样是对具体表的操作
		HTable table = new HTable(config, TN);
		Get get = new Get(Bytes.toBytes("rk_1001"));
		get.setMaxVersions(3);
		get.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("age"));
		// 定义一个ArryList
		ArrayList<Get> list = new ArrayList<Get>();
		// 将get的多条数据放入list中
		list.add(get);
		// 将list放入result数组
		Result[] result = table.get(list);
		// 循环数组再拿出每一条记录
		for (Result r : result) {
			for (Cell cell : r.rawCells()) {
				System.out.println(
						"rowkey:"+new String(r.getRow()) + "\t列族："
								+ new String(CellUtil.cloneFamily(cell)) + "\t列："
								+ new String(CellUtil.cloneQualifier(cell))+ "\t值："
								+new String(CellUtil.cloneValue(cell)));
			}
		}

	}

	/*
	 * scan的方式获取数据
	 */
	private static void scanData() throws IOException {
		// 同样是对具体表的操作
		HTable table = new HTable(config, TN);
		Scan scan = new Scan();
		scan.setMaxVersions(2);
//		scan.setStartRow(Bytes.toBytes("rk_1001"));
//	    scan.setStopRow(Bytes.toBytes("rk_1002"));
		
		// 过滤器：MUST_PASS_ONE至少满足一个；MUST_PASS_ALL必须全部都满足
		FilterList filter =new FilterList(FilterList.Operator.MUST_PASS_ALL);
		// 对RowKey进行过滤
		PrefixFilter prefixFilter =new PrefixFilter("rk_1".getBytes()); //过滤rowKey以rk_1开头的
		filter.addFilter(prefixFilter);
		// 对某列值进行过滤
		SingleColumnValueFilter singleFilter = new SingleColumnValueFilter(
				Bytes.toBytes("cf1"),
				Bytes.toBytes("age"),
				CompareOp.GREATER,	//age>20
				Bytes.toBytes("20")	//这里按照字符串比较
				);
		filter.addFilter(singleFilter);
		// 把过滤器设置到scan中
		scan.setFilter(filter);
		ResultScanner resultScanner = table.getScanner(scan);
		
		for (Result rs : resultScanner) {
			// eg:解决中文base64 加密问题 方案1（推荐使用）
			System.out.println("rowkey:" + new String(rs.getRow(),"utf-8"));
			for (Cell cell : rs.rawCells()) {
				System.out.println(
						  new String(CellUtil.cloneFamily(cell)) + "\t"
						+ new String(CellUtil.cloneQualifier(cell))+" : "
						+ new String(CellUtil.cloneValue(cell),"utf-8")
						);
			}		
			// eg:解决中文base64 加密问题 方案2
//			String addr = new String(rs.getColumnLatestCell("cf1".getBytes(),"addr".getBytes()).getValue());
//			addr = URLDecoder.decode(addr.replace("\\x", "%"), "utf-8");
//			System.out.println(addr);

		}

	}

	/*
	 * 插入数据 put
	 */
	private static void putHbase() throws IOException {
		// 插入数据是对具体表操作
		HTable hTable = new HTable(config, TN);
		Put put = new Put(Bytes.toBytes("rk_1001"));   //rowkey 1001
		// 定义一个ArryList
		ArrayList<Put> list = new ArrayList<Put>();
		// 列族	列	列的值
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("name"),Bytes.toBytes("欧阳夏"));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes("26"));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("addr"),Bytes.toBytes("南京"));
		// 将多条数据先放入list中
		list.add(put);
//		hTable.put(put);
		// 再将每个list放入表中，可以达到一次插入多行数据的效果
		hTable.put(list);
		System.out.println("---------添加记录完成-----------");
		// 完成后要关闭
		hTable.close();

	}

	/*
	 * 创建 删除表
	 */
	private static void createHbase() throws IOException {
		// 健壮性判断 是否存在 存在就删除
		// 判断表是否存在，若存在删除！
		if (hBaseAdmin.tableExists(TN)) {
			hBaseAdmin.disableTable(TN); // TableNotEnabledException
			hBaseAdmin.deleteTable(TN);
			System.out.println("表存在 已删除");
		}
		HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(TN));
		HColumnDescriptor family = new HColumnDescriptor("cf1");
		family.setBlockCacheEnabled(true);	//是否开启BlockCache
		family.setInMemory(true);	//是否加载到内存
		family.setMaxVersions(3);	//保留的版本
		descriptor.addFamily(family);
		hBaseAdmin.createTable(descriptor);
		System.out.println("------------表创建完成---------------");
	}
}
