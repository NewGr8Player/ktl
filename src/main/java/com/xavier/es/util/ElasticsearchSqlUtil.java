package com.xavier.es.util;

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

import java.util.*;

@Slf4j
@Component
public class ElasticsearchSqlUtil {

	public static String ES_HOST;
	public static String ES_PORT;

	@Value("${sync.elasticsearch.host}")
	public void setEsHost(String esHost) {
		ES_HOST = esHost;
	}

	@Value("${sync.elasticsearch.port}")
	public void setEsPort(String esPort) {
		ES_PORT = esPort;
	}

	/**
	 * 完整Es-http访问路径
	 *
	 * @return
	 */
	public static String getEsUrl() {
		return "http://" + ES_HOST + ":" + ES_PORT + "/_sql";
	}

	/**
	 * 使用sql查询结果集
	 *
	 * @param sql es-sql语句
	 * @return
	 */
	public static EsPage findPageBySql(String sql, int currentPage, int pageSize) throws Exception {
		if (currentPage < 1 || pageSize < 1) {
			throw new IllegalArgumentException("当前页码或页大小必须大于0。");
		}
		RestTemplate restTemplate = new RestTemplate();

		HttpHeaders headers = new HttpHeaders();
		headers.setContentType(MediaType.APPLICATION_JSON);
		Map<String, String> map = new HashMap();
		map.put("sql", sql + " limit " + (currentPage * pageSize));
		String requestBody = JSONObject.toJSONString(map);
		HttpEntity<String> request = new HttpEntity(requestBody, headers);
		Map apiResponse = restTemplate.postForObject(getEsUrl(), request, Map.class);
		if (null != apiResponse.get("hits")) {
			Map hints = (Map) apiResponse.get("hits");
			int recordCount = (int) hints.get("total"); /* 总数 */
			List<Map<String, Object>> recordList = Optional.ofNullable((List<Map<String, Object>>) hints.get("hits")).orElse(new ArrayList<>());
			/* 软分页 */
			int skipSize = (currentPage - 1) * pageSize;
			if (recordCount > currentPage * pageSize) { /* 总数 > 结果集大小 */
				recordList = recordList.subList(skipSize, recordList.size());
			} else {
				if(skipSize > recordCount){ /* 非法页码略过全部数据 */
					skipSize = recordCount;
				}
				if(recordList.size() > skipSize){
					recordList = recordList.subList(skipSize, recordCount);
				}
			}
			List resultList = new ArrayList<>();
			recordList.forEach(
					e -> resultList.add(e.get("_source"))
			);
			return new EsPage(currentPage, pageSize, recordCount, resultList);
		} else {
			return new EsPage();
		}

	}
}
