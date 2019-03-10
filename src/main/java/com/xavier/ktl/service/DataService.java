package com.xavier.ktl.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 数据源Service
 *
 * @author NewGr8Player
 */
@Service
public class DataService {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	/**
	 * 根据sql查询数据
	 *
	 * @param querySql 查询sql
	 * @return
	 */
	public List<Map<String, Object>> dataQuery(String querySql) {
		return jdbcTemplate.queryForList(querySql);
	}

	/**
	 * 查询数量
	 *
	 * @param countSql 查询数量sql
	 * @return
	 */
	public long countQuery(String countSql,String countFieldName) {
		Map<String, Object> result =  jdbcTemplate.queryForMap(countSql);
		return (long)result.get(countFieldName);
	}
}
