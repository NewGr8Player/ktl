package com.xavier.ktl.service;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@Slf4j
@AllArgsConstructor
public class ThredQuery implements Callable<List<Map<String, Object>>> {

	private DataService dataService;

	private String sql;

	private String indexName;
	private String typeName;

	private long bindex;
	private long num;

	@Override
	public List<Map<String, Object>> call() throws Exception {
		String q = sql + " limit " + num + " offset " + bindex;
		log.info("执行语句:{}", q);

		List<Map<String, Object>> dataList = dataService.dataQuery(q);
		if (null != dataList) {
			EsThread.test(dataList, indexName, typeName);
		}
		return dataList;
	}
}
