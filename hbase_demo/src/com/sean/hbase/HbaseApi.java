package com.sean.hbase;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.io.filefilter.PrefixFileFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sean.hbase.Phone
import com.sean.hbase.Phone.phoneDetail;

public class HbaseApi {

	HBaseAdmin admin;

	String table = "phone2";

	Random r = new Random();

	HTable htTable;

	@Before
	public void init() throws Exception {

		Configuration configuration = new Configuration();
		config.set("hbase.zookeeper.quorum", "sean01,sean02,sean03");
		long time1 = System.currentTimeMillis();
		admin = new HBaseAdmin(configuration);
		long time2 = System.currentTimeMillis();

		System.err.println(time2 - time1);

		htTable = new HTable(configuration, table);
		long time3 = System.currentTimeMillis();

		System.err.println(time3 - time2);
	}

	public static void main(String[] args) throws Exception {
		HbaseApi hbaseApi = new HbaseApi();
		hbaseApi.init();

		hbaseApi.createTable();
		
		hbaseApi.stop();
	}

	@Test
	public void createTable() throws IOException {

		HTableDescriptor hbDescriptor = new HTableDescriptor(TableName.valueOf(table));

		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("cf1");

		hColumnDescriptor.setInMemory(true);

		hColumnDescriptor.setMaxVersions(3);

		hbDescriptor.addFamily(hColumnDescriptor);
		System.out.println("hi----");
		admin.createTable(hbDescriptor);

		System.out.println("hello----");
	}

	@Test
	public void getData() throws IOException {

		Get get = new Get("5".getBytes());

		// get.addColumn("cf1".getBytes(), "name".getBytes());

		Result result = htTable.get(get);

		// System.out.println("name: " + new
		// String(result.getValue("cf1".getBytes(), "name".getBytes())));
		// System.out.println("age: " + new
		// String(result.getValue("cf1".getBytes(), "age".getBytes())));
		// System.out.println("sex: " + new
		// String(result.getValue("cf1".getBytes(), "sex".getBytes())));

		List<Cell> listCells = result.listCells();
		
		for (Cell cell : listCells) {
			System.out.print("family: " + new String(CellUtil.cloneFamily(cell)));
			System.out.print("--column: " + new String(CellUtil.cloneQualifier(cell)));
			System.out.println("--value: " + new String(CellUtil.cloneValue(cell)));

		}

	}
	
	@Test
	public void getData1() throws IOException {

		Get get = new Get("18683013771_9223370498531575807".getBytes());

		Result result = htTable.get(get);

		byte[] data = result.getValue("cf1".getBytes(), "day".getBytes());
		
		Phone.dayPhoneDetail dayphone = Phone.dayPhoneDetail.parseFrom(data);
		
		List<Phone.phoneDetail> list = dayphone.getPhoneDetailListList();
		
		for (phoneDetail phoneDetail : list) {
			System.out.println(phoneDetail.getDfphone() + "-" + phoneDetail.getType() 
					+ "-" + phoneDetail.getDate() + "-" + phoneDetail.getLength());
		}
		
	}

	/**
	 * 查询通话记录： 
	 * 手机号 对方手机号 通话时长 通话时间 主叫/被叫 rowkey:手机号_时间戳 某个手机号的某个月所有通话记录
	 * 
	 * @throws IOException
	 * @throws ParseException
	 */
	@Test
	public void scanDB1() throws IOException, ParseException {

		Scan scan = new Scan();
		String phone = "18693258201";

		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

		String startRow = phone + "_" + (Long.MAX_VALUE - sdf.parse("20180301000000").getTime());
		String stopRow = phone + "_" + (Long.MAX_VALUE - sdf.parse("20180201000000").getTime());

		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());

		ResultScanner scanner = htTable.getScanner(scan);

		for (Result result : scanner) {
			String dfphone = new String(result.getValue("cf1".getBytes(), "dfphone".getBytes()));
			String length = new String(result.getValue("cf1".getBytes(), "length".getBytes()));
			String type = new String(result.getValue("cf1".getBytes(), "type".getBytes()));
			String date = new String(result.getValue("cf1".getBytes(), "date".getBytes()));
			System.out.println(phone + "-> " + dfphone + " - - -" + date + "- - -" + type + " - " + length);
		}
	}

	/**
	 * 查看某个用户,所有 type=1的记录
	 * 
	 * @throws ParseException
	 * @throws IOException
	 */

	public void scanDB2() throws ParseException, IOException {

		Scan scan = new Scan();
		String phone = "18693258201";

		FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		PrefixFilter prefixFilter = new PrefixFilter(phone.getBytes());
		SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter("cf1".getBytes(),
				"type".getBytes(), CompareOp.EQUAL, "1".getBytes());
		
		PageFilter pageFilter = new PageFilter(3);

		filterList.addFilter(singleColumnValueFilter);
		filterList.addFilter(prefixFilter);
		filterList.addFilter(pageFilter);
		
		scan.setFilter(filterList);

		ResultScanner scanner = htTable.getScanner(scan);

		for (Result result : scanner) {
			String dfphone = new String(result.getValue("cf1".getBytes(), "dfphone".getBytes()));
			String length = new String(result.getValue("cf1".getBytes(), "length".getBytes()));
			String type = new String(result.getValue("cf1".getBytes(), "type".getBytes()));
			String date = new String(result.getValue("cf1".getBytes(), "date".getBytes()));
			System.out.println(phone + "-> " + dfphone + " - - -" + date + "- - -" + type + " - " + length);
		}

	}


	/**
	 * 随机插入10个号码，每个号码100条通话记录
	 * 
	 * @throws ParseException
	 * @throws InterruptedIOException
	 * @throws RetriesExhaustedWithDetailsException
	 */
	@Test
	public void insertDB2() throws Exception {

		List<Put> list = new ArrayList<>();

		for (int i = 0; i < 10; i++) {

			String phone = getPhone("186");

			for (int j = 0; j < 100; j++) {
				// 对方手机号
				String dfphone = getPhone("177");
				// 通话时长
				String length = r.nextInt(100) + "";
				// 主叫，被叫
				String type = r.nextInt(2) + "";

				String date = getDate("2018");

				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

				String rowkey = phone + "_" + (Long.MAX_VALUE - sdf.parse(date).getTime());

				Put put = new Put(rowkey.getBytes());

				put.add("cf1".getBytes(), "dfphone".getBytes(), dfphone.getBytes());
				put.add("cf1".getBytes(), "length".getBytes(), length.getBytes());
				put.add("cf1".getBytes(), "type".getBytes(), type.getBytes());
				put.add("cf1".getBytes(), "date".getBytes(), date.getBytes());

				list.add(put);
			}
		}

		htTable.put(list);
	}
	/**
	 * 随机插入10个号码
	 * 每个号码，一天产生100个通话记录
	 * 
	 * @throws ParseException
	 * @throws InterruptedIOException
	 * @throws RetriesExhaustedWithDetailsException
	 */
	public void insertDB3() throws Exception {


		List<Put> list = new ArrayList<>();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		
		
		for (int i = 0; i < 10; i++) {

			String phone = getPhone("186");
			
			Phone.dayPhoneDetail.Builder  dayPhoneDetail = Phone.dayPhoneDetail.newBuilder();

			String rowkey = phone + "_" + (Long.MAX_VALUE - sdf.parse("20181001000000").getTime());
			
			for (int j = 0; j < 100; j++) {
				// 对方手机号
				String dfphone = getPhone("177");
				// 通话时长
				String length = r.nextInt(100) + "";
				// 主叫，被叫
				String type = r.nextInt(2) + "";

				String date = getDate2("20181001");
				
				Phone.phoneDetail.Builder phoneDetail = Phone.phoneDetail.newBuilder();
				
				phoneDetail.setDate(date);
				phoneDetail.setDfphone(dfphone);
				phoneDetail.setLength(length);
				phoneDetail.setType(type);
				
				dayPhoneDetail.addPhoneDetailList(phoneDetail);
				
			}
			
			Put put = new Put(rowkey.getBytes());
			put.add("cf1".getBytes(), "day".getBytes(), dayPhoneDetail.build().toByteArray());
			
			list.add(put);
		}

		htTable.put(list);
	}


	/**
	 * 随机生成手机号，前缀prefix
	 */
	@Test
	public String getPhone(String prefix) {

		return prefix + String.format("%08d", r.nextInt(99999999));
	}

	/**
	 * 生成时间：yyyyMMddHHmmss
	 * 
	 * @param year
	 *            传入年
	 * @return date 日期
	 */
	@Test
	public String getDate(String year) {
		return year + String.format("%02d%02d%02d%02d%02d",
				new Object[] { r.nextInt(12), r.nextInt(31), r.nextInt(24), r.nextInt(60), r.nextInt(60) });
	}

	/**
	 * 生成时间：yyyyMMddHHmmss
	 * 
	 * @param ymd
	 *            传入年月日
	 * @return date 日期
	 */
	@Test
	public String getDate2(String ymd) {
		return ymd + String.format("%02d%02d%02d", new Object[] { r.nextInt(24), r.nextInt(60), r.nextInt(60) });

	}

	/**
	 * 插入数据
	 * 
	 * @throws Exception，
	 */
	@Test
	public void insertDB() throws Exception {

		byte[] rowkey = "5".getBytes();
		Put put = new Put(rowkey);

        put.add("cf1".getBytes(), "name".getBytes(), "zhangsan".getBytes());
		put.add("cf1".getBytes(), "age".getBytes(), "28".getBytes());
		put.add("cf1".getBytes(), "sex".getBytes(), "man".getBytes());
		htTable.put(put);
	}

	/**
	 * 预分区
	 * @throws IOException
	 */
	public void createTable2() throws IOException{

		HTableDescriptor hbDescriptor = new HTableDescriptor(TableName.valueOf("table4"));
		HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("cf1");

		hbDescriptor.addFamily(hColumnDescriptor);

		admin.createTable(hbDescriptor, getPhoneSplitKeys());

    }
	
	/**
	 * 
	 * @param
	 * @return
	 */
	public byte[][] getPhoneSplitKeys() {

        //【【133】，【149】】
        byte[][] splitKeys = new byte[8][];

        String[] prefix_phones = {"133","149","153","173","177","180","181","189","199"};
        //由于hbase的分区是左闭又开的，所以for循环应该是从1开始，也就是生成的第一个分区是负无穷到149,这个分区包含了133
        for (int i = 1; i <9; i++) {
        	System.out.println(i);
            splitKeys[i-1 ] = prefix_phones[i].getBytes();
        }
        return splitKeys;
    }
	
	
	/**
	 * 生成rowkey
	 * @param
	 * @return
	 */
    public byte[] generateHashRowkey() {
    	String date = getDate("2018");
    	
    	System.out.println(date);
    	//对date先计算出hash值，然后只取前8位
        String hash = MD5Hash.getMD5AsHex(date.getBytes()).substring(0, 8);
        //rowkey  = hash  + date  拼凑起来。
        return Bytes.add(Bytes.toBytes(hash), Bytes.toBytes(date));

    }

    /**
     * 指定要分区的个数，生成一批rowkey
     * 业务场景：rowkey是递增的 ，比如rowkey是date
     * @param partition
     * @return
     */
    public byte[][] getHashSplitKeys(int partition) {

        byte[][] splitKeys = new byte[partition - 1][];
        
        for (int i = 1; i < partition; i++) {

            splitKeys[i - 1] = generateHashRowkey();
        }
        return splitKeys;
    }

	@After
	public void stop() throws IOException {
		if (admin != null) {
			admin.close();
		}

		if (htTable != null) {
			htTable.close();
		}
	}

    /**
     * 过滤器
     */
    @Test
    public void filter() {

        Scan scan = new Scan();
//        Filter filter1 = new PrefixFilter(Bytes.toBytes("4"));//前缀过滤器,筛选出具有特定前缀的行键的数据
//        Filter filter = new KeyOnlyFilter();//只返回行，不返回Column的值
//        Filter filter = new InclusiveStopFilter(Bytes.toBytes("7"));//扫描到这一行停止。
//        Filter filter = new FirstKeyOnlyFilter();//扫描到每行的第一个列就返回.
//        Filter filter = new ColumnPrefixFilter(Bytes.toBytes("mo")); // 筛选出前缀匹配的
//        Filter filter = new ValueFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator("zhangsan"));//按值筛选出列
//        Filter skf = new SkipFilter(filter);//发现某一行中的一列需要过滤时，整个行就会被过滤掉
//        Filter filter = new ColumnCountGetFilter(2);//每行最多返回多少个列.如果突然发现一行中的列数超过设定的最大值时，整个扫描操作会停止.

        SingleColumnValueFilter filter = new SingleColumnValueFilter(
                Bytes.toBytes("cf1"),
                Bytes.toBytes("money"),
                CompareFilter.CompareOp.EQUAL,
                new SubstringComparator("100"));//用一列的值决定这一行的数据是否被过滤
        filter.setFilterIfMissing(false);//默认为false.
        List<Filter> filters = new ArrayList<>();
        filters.add(filter1);
        filters.add(filter2);

//        FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ONE, filters);//用于综合使用多个过滤器
//        Filter filter = new PageFilter(3);
        scan.setFilter(filter);

        try {
            ResultScanner resultScanner = htTable.getScanner(scan);
            for (Result rs : resultScanner) {

                String rowkey = new String(rs.getRow());

                for (Cell cell : rs.rawCells()) {
                    String family = new String(CellUtil.cloneFamily(cell));
                    String column = new String(CellUtil.cloneQualifier(cell));
                    String value = new String(CellUtil.cloneValue(cell));
                    System.out.println(rowkey + " : " + family + " : " + column + " : " + value);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}
