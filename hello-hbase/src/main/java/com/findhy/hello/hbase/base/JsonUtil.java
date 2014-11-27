package com.findhy.hello.hbase.base;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import net.sf.ezmorph.MorpherRegistry;
import net.sf.ezmorph.object.DateMorpher;
import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;
import net.sf.json.util.JSONUtils;
import net.sf.json.util.PropertyFilter;

public class JsonUtil {
	public static final String YYYY_MM_DD_HH_MM_ss="yyyy-MM-dd HH:mm:ss";
	public static final String YYYY_MM_DD="yyyy-MM-dd";
	/**
	 * 设置日期转换格式
	 */
	static {
		// 注册器
		MorpherRegistry mr = JSONUtils.getMorpherRegistry();
		// 可转换的日期格式，即Json串中可以出现以下格式的日期与时间
		DateMorpher dm = new DateMorpher(
				new String[] { YYYY_MM_DD_HH_MM_ss,YYYY_MM_DD });
		mr.registerMorpher(dm);
	}

	/**
	 * 从json串转换成实体对象
	 * 
	 * @param jsonObjStr
	 *            e.g. {'name':'get','dateAttr':'2009-11-12'}
	 * @param clazz
	 *            Person.class
	 * @return
	 */
	public static Object getDtoFromJsonObjStr(String jsonObjStr, Class<?> clazz) {
		return JSONObject.toBean(JSONObject.fromObject(jsonObjStr), clazz);
	}

	/**
	 * 从json串转换成实体对象，并且实体集合属性存有另外实体Bean
	 * 
	 * @param jsonObjStr
	 *            e.g. {'data':[{'name':'get'},{'name':'set'}]}
	 * @param clazz
	 *            e.g. MyBean.class
	 * @param classMap
	 *            e.g. classMap.put("data", Person.class)
	 * @return Object
	 */
	public static Object getDtoFromJsonObjStr(String jsonObjStr, Class<?> clazz,
			Map<Object,Object> classMap) {
		return JSONObject.toBean(JSONObject.fromObject(jsonObjStr), clazz,
				classMap);
	}

	/**
	 * 把一个json数组串转换成普通数组
	 * 
	 * @param jsonArrStr
	 *            e.g. ['get',1,true,null]
	 * @return Object[]
	 */
	public static Object[] getArrFromJsonArrStr(String jsonArrStr) {
		return JSONArray.fromObject(jsonArrStr).toArray();
	}

	/**
	 * 把一个json数组串转换成实体数组
	 * 
	 * @param jsonArrStr
	 *            e.g. [{'name':'get'},{'name':'set'}]
	 * @param clazz
	 *            e.g. Person.class
	 * @return Object[]
	 */
	public static Object[] getDtoArrFromJsonArrStr(String jsonArrStr,
			Class<?> clazz) {
		JSONArray jsonArr = JSONArray.fromObject(jsonArrStr);
		Object[] objArr = new Object[jsonArr.size()];
		for (int i = 0; i < jsonArr.size(); i++) {
			objArr[i] = JSONObject.toBean(jsonArr.getJSONObject(i), clazz);
		}
		return objArr;
	}

	/**
	 * 把一个json数组串转换成实体数组，且数组元素的属性含有另外实例Bean
	 * 
	 * @param jsonArrStr
	 *            e.g. [{'data':[{'name':'get'}]},{'data':[{'name':'set'}]}]
	 * @param clazz
	 *            e.g. MyBean.class
	 * @param classMap
	 *            e.g. classMap.put("data", Person.class)
	 * @return Object[]
	 */
	public static Object[] getDtoArrFromJsonArrStr(String jsonArrStr,
			Class<?> clazz, Map<Object,Object> classMap) {
		JSONArray array = JSONArray.fromObject(jsonArrStr);
		Object[] obj = new Object[array.size()];
		for (int i = 0; i < array.size(); i++) {
			JSONObject jsonObject = array.getJSONObject(i);
			obj[i] = JSONObject.toBean(jsonObject, clazz, classMap);
		}
		return obj;
	}

	/**
	 * 把一个json数组串转换成存放普通类型元素的集合
	 * 
	 * @param jsonArrStr
	 *            e.g. ['get',1,true,null]
	 * @return List
	 */
	public static List<Object> getListFromJsonArrStr(String jsonArrStr) {
		JSONArray jsonArr = JSONArray.fromObject(jsonArrStr);
		List<Object> list = new ArrayList<Object>();
		for (int i = 0; i < jsonArr.size(); i++) {
			list.add(jsonArr.get(i));
		}
		return list;
	}

	/**
	 * 把一个json数组串转换成集合，且集合里存放的为实例Bean
	 * 
	 * @param jsonArrStr
	 *            e.g. [{'name':'get'},{'name':'set'}]
	 * @param clazz
	 * @return List
	 */
	public static List<Object> getListFromJsonArrStr(String jsonArrStr, Class<?> clazz) {
		JSONArray jsonArray = JSONArray.fromObject(jsonArrStr);
        List<Object> list = (List<Object>) JSONArray.toCollection(jsonArray, clazz);
        return list;
	}
	
	public static List<?> getCaseListFromJsonArrStr(String jsonArrStr, Class<?> clazz) {
		JSONArray jsonArray = JSONArray.fromObject(jsonArrStr);
        List<?> list = (List<?>) JSONArray.toCollection(jsonArray, clazz);
        return list;
	}

	/**
	 * 把一个json数组串转换成集合，且集合里的对象的属性含有另外实例Bean
	 * 
	 * @param jsonArrStr
	 *            e.g. [{'data':[{'name':'get'}]},{'data':[{'name':'set'}]}]
	 * @param clazz
	 *            e.g. MyBean.class
	 * @param classMap
	 *            e.g. classMap.put("data", Person.class)
	 * @return List
	 */
	public static List<Object> getListFromJsonArrStr(String jsonArrStr, Class<?> clazz,
			Map<Object,Object> classMap) {
		JSONArray jsonArr = JSONArray.fromObject(jsonArrStr);
		List<Object> list = new ArrayList<Object>();
		for (int i = 0; i < jsonArr.size(); i++) {
			list.add(JSONObject.toBean(jsonArr.getJSONObject(i), clazz,
					classMap));
		}
		return list;
	}

	/**
	 * 把json对象串转换成map对象
	 * 
	 * @param jsonObjStr
	 *            e.g. {'name':'get','int':1,'double',1.1,'null':null}
	 * @return Map
	 */
	public static Map<Object,Object> getMapFromJsonObjStr(String jsonObjStr) {
		JSONObject jsonObject = JSONObject.fromObject(jsonObjStr);

		Map<Object,Object> map = new HashMap<Object,Object>();
		for (Iterator<?> iter = jsonObject.keys(); iter.hasNext();) {
			String key = (String) iter.next();
			map.put(key, jsonObject.get(key));
		}
		return map;
	}

	/**
	 * 把json对象串转换成map对象，且map对象里存放的为其他实体Bean
	 * 
	 * @param jsonObjStr
	 *            e.g. {'data1':{'name':'get'},'data2':{'name':'set'}}
	 * @param clazz
	 *            e.g. Person.class
	 * @return Map
	 */
	public static Map<Object,Object> getMapFromJsonObjStr(String jsonObjStr, Class<?> clazz) {
		JSONObject jsonObject = JSONObject.fromObject(jsonObjStr);

		Map<Object,Object> map = new HashMap<Object,Object>();
		for (Iterator<?> iter = jsonObject.keys(); iter.hasNext();) {
			String key = (String) iter.next();
			map.put(key,
					JSONObject.toBean(jsonObject.getJSONObject(key), clazz));
		}
		return map;
	}

	/**
	 * 把json对象串转换成map对象，且map对象里存放的其他实体Bean还含有另外实体Bean
	 * 
	 * @param jsonObjStr
	 *            e.g. {'mybean':{'data':[{'name':'get'}]}}
	 * @param clazz
	 *            e.g. MyBean.class
	 * @param classMap
	 *            e.g. classMap.put("data", Person.class)
	 * @return Map
	 */
	public static Map<Object,Object> getMapFromJsonObjStr(String jsonObjStr, Class<?> clazz,
			Map<Object,Object> classMap) {
		JSONObject jsonObject = JSONObject.fromObject(jsonObjStr);

		Map<Object,Object> map = new HashMap<Object,Object>();
		for (Iterator<?> iter = jsonObject.keys(); iter.hasNext();) {
			String key = (String) iter.next();
			map.put(key, JSONObject.toBean(jsonObject.getJSONObject(key),
					clazz, classMap));
		}
		return map;
	}

	/**
	 * 把实体Bean、Map对象、数组、列表集合转换成Json串
	 * 
	 * @param obj
	 * @return
	 * @throws Exception
	 *             String
	 */
	public static String getJsonStr(Object obj) {
		return getJsonStr(obj,true,true);
	}
	public static String getJsonStr(Object obj,Boolean skipEmpty) {
		return getJsonStr(obj,skipEmpty,true);
	}
	public static String getJsonStr(Object obj,Boolean skipEmpty,Boolean useIgnore) {
		String jsonStr = null;
		// Json配置
		JsonConfig jsonCfg = new JsonConfig();

		if(skipEmpty){			
			jsonCfg.setJsonPropertyFilter(new PropertyFilter() {
	            @Override  
	            public boolean apply(Object source, String name, Object value) {  
	                if(value==null){  
	                    return true ;  
	                }  
	                return false;  
	            }          
	        });	
		}
		
		if (obj == null) {
			return "{}";
		}
		if(obj.getClass()==String.class){
			return "{str:\""+obj+"\"}";	
		}

		if (obj instanceof Collection || obj instanceof Object[]) {
			jsonStr = JSONArray.fromObject(obj, jsonCfg).toString();
		} else {
			jsonStr = JSONObject.fromObject(obj, jsonCfg).toString();
		}

		return jsonStr;
	}

}
