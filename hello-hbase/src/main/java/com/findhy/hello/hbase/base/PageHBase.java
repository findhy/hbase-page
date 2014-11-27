package com.cy.ad.union.common.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * hbase分页类
 * 
 * hbase分页与其它数据库分页的关键点在于，无法获取条件范围内的数据总量以及没有类似于Oracle的rownum的行标记
 * 
 * 所以在实现的时候要记住每一页的startRow和下一页的startRow，
 * 通过scan的row范围查询再结合PageFilter过滤器来实现分页效果
 * 
 * @author sunwei_oversea
 *
 */
public class PageHBase {

	private int currentPageNo=1;//当前页码
	private int pageSize=3;//每页显示行数
	private int totalCount;//总行数
	private int totalPage;//总页数
	private int direction;//下一页：1 上一页2
	private boolean hasNext=false;//是否有下一页
	private String nextPageRowkey;//下一页起始rowkey
	private List<Map<String, String>> resultList;//结果集List
	private Map<String,String> paramMap=new HashMap<String,String>();//分页查询参数
	private Map<Integer,String> pageStartRowMap=new HashMap<Integer,String>();//每页对应的startRow，key为currentPageNo，value为Rowkey
	private Scan scan=new Scan();
	
	public Scan getScan(String startRowkey,String endRowkey){
		scan.setCaching(100);
		if(direction==1&&hasNext){
			scan.setStartRow(Bytes.toBytes(startRowkey));
			scan.setStopRow(Bytes.toBytes(endRowkey));
		}else{
			if(pageStartRowMap.get(currentPageNo)!=null){
				scan.setStartRow(Bytes.toBytes(pageStartRowMap.get(currentPageNo)));
				scan.setStopRow(Bytes.toBytes(endRowkey));
			}else{
				scan.setStartRow(Bytes.toBytes(startRowkey));
				scan.setStopRow(Bytes.toBytes(endRowkey));
			}
		}
		this.hasNext=false;
		this.nextPageRowkey=null;
		return scan;
	}
	
	public int getCurrentPageNo() {
		return currentPageNo;
	}

	public void setCurrentPageNo(int currentPageNo) {
		this.currentPageNo = currentPageNo;
	}

	public int getPageSize() {
		return pageSize;
	}

	public void setPageSize(int pageSize) {
		this.pageSize = pageSize;
	}

	public int getTotalCount() {
		return totalCount;
	}

	public void setTotalCount(int totalCount) {
		this.totalCount = totalCount;
	}

	public int getTotalPage() {
		return totalPage;
	}

	public void setTotalPage(int totalPage) {
		this.totalPage = totalPage;
	}

	public int getDirection() {
		return direction;
	}

	public void setDirection(int direction) {
		this.direction = direction;
	}

	public boolean isHasNext() {
		return hasNext;
	}

	public void setHasNext(boolean hasNext) {
		this.hasNext = hasNext;
	}

	public String getNextPageRowkey() {
		return nextPageRowkey;
	}

	public void setNextPageRowkey(String nextPageRowkey) {
		this.nextPageRowkey = nextPageRowkey;
	}

	public List<Map<String, String>> getResultList() {
		return resultList;
	}

	public void setResultList(List<Map<String, String>> resultList) {
		this.resultList = resultList;
	}

	public Map<String, String> getParamMap() {
		return paramMap;
	}

	public void setParamMap(Map<String, String> paramMap) {
		this.paramMap = paramMap;
	}

	public Map<Integer, String> getPageStartRowMap() {
		return pageStartRowMap;
	}

	public void setPageStartRowMap(Map<Integer, String> pageStartRowMap) {
		this.pageStartRowMap = pageStartRowMap;
	}

	public void execute() {
		int n = totalCount / pageSize;
		if (totalCount % pageSize == 0) {
			totalPage = n;
		} else {
			totalPage = ((int) n) + 1;
		}
	}
}
