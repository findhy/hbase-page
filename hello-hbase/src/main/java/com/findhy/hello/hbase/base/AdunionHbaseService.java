package com.findhy.hello.hbase.base;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * put 'adunion_active',1234,'click:log',"{\"active_id\":\"1234\",\"offer_id\":\"abc\"}"
 * put 'adunion_active',5678,'click:log',"{\"active_id\":\"1234\",\"offer_id\":\"dfe\"}"
 * 
 * @author sunwei_oversea
 *
 */
public class AdunionHbaseService {

	private static final Logger log = LoggerFactory
			.getLogger(AdunionHbaseService.class);
	public static final String CLICK_COLUMN_NAME="c";
	public static final String CLICK_COLUMN_KEY="v";
	public static final String POSTBACK_COLUMN_NAME="p";
	public static final String POSTBACK_COLUMN_KEY="v";
	public static final String ADUNION_TABLE_NAME="adunion_active";
	private Configuration configuration;
	private String clientPort;
	private String retriesNumber;
	private String zookeeperQuorum;
	
	public AdunionHbaseService(){
		try {
			configuration = HBaseConfiguration.create();
			this.setClientPort("2181");
			this.setRetriesNumber("1");
			this.setZookeeperQuorum("127.0.0.1");
			configuration.set("hbase.zookeeper.property.clientPort", this.getClientPort());
			configuration.set("hbase.client.retries.number", this.getRetriesNumber());
			configuration.set("hbase.zookeeper.quorum",this.getZookeeperQuorum());
		} catch (Exception e) {
			log.error(e.getMessage());
		}
	}
	
	public PageHBase getPageHBaseData(String tableName, String columnName,String columnKey,String businessId,String publisherId,String offerId,
			Date startDate,Date endDate, PageHBase pager) {
		log.debug("startDate:"+startDate.toLocaleString()+"endDate"+endDate.toLocaleString());
		HConnection con = null;
		HTable table = null;
		ResultScanner rs=null;
		List<Map<String,String>> resultList=new ArrayList<Map<String,String>>();
		try {
			if(startDate==null||endDate==null) return pager;
			
			con = HConnectionManager.createConnection(configuration);
			table=(HTable)con.getTable(tableName);
			
			StringBuffer startRow=new StringBuffer();
			StringBuffer endRow=new StringBuffer();
			startRow.append(startDate.getTime());
			endRow.append(endDate.getTime());
			if(pager.getNextPageRowkey()==null) pager.setNextPageRowkey(startRow.toString());
			Scan scan=pager.getScan(pager.getNextPageRowkey(),endRow.toString());
			
			List<Filter> filters=new ArrayList<Filter>();
			
			if(StringUtils.isNotBlank(publisherId)){
				SingleColumnValueFilter filter=new SingleColumnValueFilter(
						Bytes.toBytes(columnName),
						Bytes.toBytes("p"),
						CompareFilter.CompareOp.EQUAL,
						Bytes.toBytes(publisherId));
				filters.add(filter);
			}
			if(StringUtils.isNotBlank(offerId)){
				SingleColumnValueFilter filter=new SingleColumnValueFilter(
						Bytes.toBytes(columnName),
						Bytes.toBytes("o"),
						CompareFilter.CompareOp.EQUAL,
						Bytes.toBytes(offerId));
				filters.add(filter);
			}
			if(StringUtils.isNotBlank(businessId)){
				SingleColumnValueFilter filter=new SingleColumnValueFilter(
						Bytes.toBytes(columnName),
						Bytes.toBytes("b"),
						CompareFilter.CompareOp.EQUAL,
						Bytes.toBytes(businessId));
				filters.add(filter);
			}
			Filter pageFilter=new PageFilter(pager.getPageSize()+1);
			Filter familyFilter=new FamilyFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(columnName)));
			Filter qualifierFilter=new QualifierFilter(CompareFilter.CompareOp.EQUAL,
					new BinaryComparator(Bytes.toBytes(columnKey)));
			filters.add(familyFilter);
			filters.add(qualifierFilter);
			filters.add(pageFilter);
			FilterList filterList=new FilterList(filters);
			scan.setFilter(filterList);
			rs = table.getScanner(scan);
			int totalRow=0;
			if(rs!=null){
				for(Result result : rs){
					totalRow++;
					if(totalRow==1){
						pager.getPageStartRowMap().put(pager.getCurrentPageNo(),Bytes.toString(result.getRow()));
	                    pager.setTotalPage(pager.getPageStartRowMap().size());
	                }
					if(totalRow>pager.getPageSize()){
						pager.setNextPageRowkey(new String(result.getRow()));
						pager.setHasNext(true);
					}else{
						Map<String,String> map = new HashMap<String,String>();
						for(KeyValue keyValue:result.raw()){
							String family=new String(keyValue.getFamily());
							byte[] value=keyValue.getValue();
							//map.put("rowkey", new String(result.getRow()));
							Map<Object,Object> jsonMap=JsonUtil.getMapFromJsonObjStr(new String(value));
							for(Object obj:jsonMap.keySet()){
								map.put(obj.toString(), jsonMap.get(obj).toString());
							}
						}
						resultList.add(map);
					}
				}
			}
			pager.setResultList(resultList);
		}catch(IOException ioe){
			log.error(ioe.getMessage());
		} catch (Exception e) {
			log.error(e.getMessage());
		}finally{
			try {
				rs.close();
			} catch (Exception e) {
				log.error(e.getMessage());
			}
			try {
				table.close();
			} catch (Exception e) {
				log.error(e.getMessage());
			}
			try {
				con.close();
			} catch (Exception e) {
				log.error(e.getMessage());
			}
		}
		return pager;
	}
	
	public void getByRowKey() throws Exception {
		HConnection con = HConnectionManager.createConnection(configuration);
		HTable table = (HTable) con.getTable("adunion_active");
		Get get = new Get(Bytes.toBytes("123"));
		get.addColumn(Bytes.toBytes("click"), Bytes.toBytes("log"));
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw()) {
			System.out.println("--------------------" + new String(kv.getRow())
					+ "----------------------------");
			System.out.println("Column Family: " + new String(kv.getFamily()));
			System.out
					.println("Column       :" + new String(kv.getQualifier()));
			System.out.println("value        : " + new String(kv.getValue()));
		}
		con.close();
	}

	public String getClientPort() {
		return clientPort;
	}

	public void setClientPort(String clientPort) {
		this.clientPort = clientPort;
	}

	public String getRetriesNumber() {
		return retriesNumber;
	}

	public void setRetriesNumber(String retriesNumber) {
		this.retriesNumber = retriesNumber;
	}

	public String getZookeeperQuorum() {
		return zookeeperQuorum;
	}

	public void setZookeeperQuorum(String zookeeperQuorum) {
		this.zookeeperQuorum = zookeeperQuorum;
	}
	
	/*public static void main(String args[]) throws Exception{
	SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd");
	System.out.println(format.parse("2014-11-25").getTime());
	}
	
	public static void main(String[] args) throws Exception {
		SimpleDateFormat format = new SimpleDateFormat( "yyyy-MM-dd");
		System.out.println(format.parse("2014-11-20").getTime()/100000);
		PageHBase pager=new PageHBase();
		pager = getPageHBaseData("adunion_active","p123","",format.parse("2014-11-20"),format.parse("2014-11-22"),pager);
		List<Map<String,String>> resultList = pager.getResultList();
		System.out.println("---------------------begin--------------------------------");
		System.out.println("---------------------Page one--------------------------------");
		for(Map<String,String> map:resultList){
			for(String key:map.keySet()){
				System.out.print("--"+key+":"+map.get(key)+"--");
			}
			System.out.println();
		}
		System.out.println("##########currentPageNo:"+pager.getCurrentPageNo()+"##########pageSize:"+pager.getPageSize()+"############");
		System.out.println("---------------------Page two--------------------------------");
		pager.setCurrentPageNo(2);
		pager.setDirection(1);
		pager = getPageHBaseData("adunion_active","p123","",format.parse("2014-11-20"),format.parse("2014-11-22"),pager);
		resultList = pager.getResultList();
		for(Map<String,String> map:resultList){
			for(String key:map.keySet()){
				System.out.print("--"+key+":"+map.get(key)+"--");
			}
			System.out.println();
		}
		System.out.println("##########currentPageNo:"+pager.getCurrentPageNo()+"##########pageSize:"+pager.getPageSize()+"############");
		System.out.println("---------------------Page three--------------------------------");
		pager.setCurrentPageNo(3);
		pager.setDirection(1);
		pager = getPageHBaseData("adunion_active","p124","",format.parse("2014-11-20"),format.parse("2014-11-22"),pager);
		resultList = pager.getResultList();
		for(Map<String,String> map:resultList){
			for(String key:map.keySet()){
				System.out.print("--"+key+":"+map.get(key)+"--");
			}
			System.out.println();
		}
		System.out.println("##########currentPageNo:"+pager.getCurrentPageNo()+"##########pageSize:"+pager.getPageSize()+"############");
		System.out.println("---------------------Page four--------------------------------");
		pager.setCurrentPageNo(4);
		pager.setDirection(1);
		pager = getPageHBaseData("adunion_active","p124","",format.parse("2014-11-20"),format.parse("2014-11-22"),pager);
		resultList = pager.getResultList();
		for(Map<String,String> map:resultList){
			for(String key:map.keySet()){
				System.out.print("--"+key+":"+map.get(key)+"--");
			}
			System.out.println();
		}
		System.out.println("##########currentPageNo:"+pager.getCurrentPageNo()+"##########pageSize:"+pager.getPageSize()+"############");
		System.out.println("---------------------end--------------------------------");
		
		}*/
}
