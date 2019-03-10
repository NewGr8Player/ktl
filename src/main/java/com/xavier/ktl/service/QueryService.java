package com.xavier.ktl.service;

import com.xavier.ktl.bean.QuerySql;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Service
public class QueryService {

	@Autowired
	private QuerySql querySql;
	@Autowired
	private DataService dataService;

	public void dealData() throws Exception {
		log.info("开始处理数据");
		for (QuerySql.QueryInfo queryInfo : querySql.getSql()) {

			long count = dataService.countQuery(queryInfo.getCountSql(), "total");

			long num = 10000;//每次查询的条数
			//需要查询的次数
			long times = count / num;
			if (count % num != 0) {
				times = times + 1;
			}
			//开始查询的行数
			long bindex = 0;

			List<Callable<List<Map<String, Object>>>> tasks = new ArrayList<Callable<List<Map<String, Object>>>>();//添加任务
			for (int i = 0; i < times; i++) {
				Callable<List<Map<String, Object>>> qfe = new ThredQuery(dataService, queryInfo.getQuerySql(), queryInfo.getIndexName(), queryInfo.getTypeName(), bindex, num);
				tasks.add(qfe);
				bindex = bindex + num;
			}
			//定义固定长度的线程池  防止线程过多
			ExecutorService execservice = Executors.newFixedThreadPool(querySql.getMaxPageNum());

			execservice.invokeAll(tasks);

			execservice.shutdown();
		}
	}
}
