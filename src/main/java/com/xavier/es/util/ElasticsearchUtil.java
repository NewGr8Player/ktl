package com.xavier.es.util;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.*;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.*;


/**
 * Elasticsearch 工具类
 * (此处Order注解不能删除，默认加载顺序该实例初始化晚于调用，造成空指针异常)
 *
 * @author NewGr8Player
 **/
@Slf4j
@Order(Ordered.HIGHEST_PRECEDENCE)
@Component
public class ElasticsearchUtil {


	@Autowired
	private RestHighLevelClient restHighLevelClient;

	private static RestHighLevelClient client;

	@PostConstruct
	public void init() {
		client = this.restHighLevelClient;
	}

	/**
	 * 判断索引是否存在
	 *
	 * @param index
	 * @return
	 */
	public static boolean isIndexExist(String index) throws IOException {
		GetIndexRequest request = new GetIndexRequest();
		request.indices(index);
		boolean exists = client.indices().exists(request, RequestOptions.DEFAULT);
		log.info("【查询索引】索引{}是否存在: {}", index, exists);
		return exists;
	}

	/**
	 * 创建索引
	 *
	 * @param index
	 * @return
	 */
	public static boolean createIndex(String index) throws IOException {
		if (isIndexExist(index)) {
			log.warn("【创建索引】索引已存在:{}", index);
			return true;
		} else {
			log.info("【创建索引】创建索引:{}", index);
			CreateIndexRequest request = new CreateIndexRequest(index);
			CreateIndexResponse createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
			return createIndexResponse.isAcknowledged();
		}
	}

	/**
	 * 删除索引
	 *
	 * @param index
	 * @return
	 */
	public static boolean deleteIndex(String index) throws IOException {
		if (!isIndexExist(index)) {
			log.info("【删除索引】索引不存在:{}", index);
			return true;
		} else {
			try {
				client.indices().delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT);
				log.info("【删除索引】删除索引:{}", index);
				return true;
			} catch (IOException e) {
				log.info("【删除索引】删除失败:{}", index);
				log.error(JSON.toJSONString(e.getStackTrace()));
				return false;
			}
		}
	}

	/**
	 * 数据添加，正定ID
	 *
	 * @param jsonObject 要增加的数据
	 * @param index  索引，类似数据库
	 * @param type       类型，类似表
	 * @param id         数据ID
	 * @return
	 */
	public static String addData(JSONObject jsonObject, String index, String type, String id) throws Exception {
		if (!isIndexExist(index)) {
			createIndex(index);
		}
		if (StringUtils.isBlank(id)) {
			id = uuid();
			log.warn("【插入数据】未传入Id，重新生成Id:{}", id);
		}
		IndexRequest indexRequest = new IndexRequest(index, type, id).source(jsonObject, XContentType.JSON);
		IndexResponse ret = client.index(indexRequest, RequestOptions.DEFAULT);
		log.info("【插入数据】Index:{},Type:{},Id:{},Result:{}", index, type, ret.getId(), ret.getResult());
		return ret.getId();
	}

	/**
	 * 数据添加
	 *
	 * @param jsonObject 要增加的数据
	 * @param index      索引，类似数据库
	 * @param type       类型，类似表
	 * @return
	 */
	public static String addData(JSONObject jsonObject, String index, String type) throws Exception {
		return addData(jsonObject, index, type, uuid());
	}

	/**
	 * 通过ID删除数据
	 *
	 * @param index 索引，类似数据库
	 * @param type  类型，类似表
	 * @param id    数据ID
	 */
	public static void deleteDataById(String index, String type, String id) throws IOException {
		if (!isIndexExist(index)) {
			log.info("【删除索引】索引不存在:{}", index);
		} else {
			DeleteRequest deleteRequest = new DeleteRequest(index, type, id);
			DeleteResponse ret = client.delete(deleteRequest, RequestOptions.DEFAULT);
			log.info("【删除数据】:Index:{},Type:{},Id:{},Result:{}", index, type, id, ret.getResult());
		}
	}

	/**
	 * 通过ID 更新数据
	 *
	 * @param jsonObject 要增加的数据
	 * @param index  索引，类似数据库
	 * @param type       类型，类似表
	 * @param id         数据ID
	 * @return
	 */
	public static void updateDataById(JSONObject jsonObject, String index, String type, String id) throws IOException {
		if (!isIndexExist(index)) {
			createIndex(index);
		}
		UpdateRequest updateRequest = new UpdateRequest(index, type, id)
				.doc(jsonObject)
				.upsert(jsonObject);/* 如果没找到就插入一条新数据 */
		UpdateResponse ret = client.update(updateRequest, RequestOptions.DEFAULT);
		log.info("【更新数据】Index:{},Type:{},Id:{},Result:{}", index, type, id, ret.getResult());
	}

	/**
	 * 通过ID获取数据
	 *
	 * @param index  索引，类似数据库
	 * @param type   类型，类似表
	 * @param id     数据ID
	 * @param fields 需要显示的字段，逗号分隔（缺省为全部字段）
	 * @return
	 */
	public static Map<String, Object> searchDataById(String index, String type, String id, String fields) throws IOException {
		if (!isIndexExist(index)) {
			log.warn("【查询数据】查询失败，索引不存在:{}", index);
			return new HashMap<>();
		}
		GetRequest getRequest = new GetRequest(index, type, id)
				.fetchSourceContext(new FetchSourceContext(true, Optional.ofNullable(fields).orElse("").split(","), null));
		GetResponse ret = client.get(getRequest, RequestOptions.DEFAULT);
		return ret.getSourceAsMap();
	}

	/**
	 * 使用分词查询
	 *
	 * @param index              索引名称
	 * @param type               类型名称,可传入多个type逗号分隔
	 * @param startTime          开始时间
	 * @param endTime            结束时间
	 * @param size               文档大小限制
	 * @param fields             需要显示的字段，逗号分隔（缺省为全部字段）
	 * @param sortFieldList      排序字段
	 * @param matchPhrase        true 使用，短语精准匹配
	 * @param highlightFieldList 高亮字段
	 * @param matchStr           过滤条件（xxx=111,aaa=222）
	 * @return
	 */
	public static List<Map<String, Object>> searchListData(String index, String type, long startTime, long endTime, Integer size, String fields, List<Map<String, String>> sortFieldList, boolean matchPhrase, List<String> highlightFieldList, String matchStr) throws Exception {

		SearchRequest searchRequest = new SearchRequest(index);
		if (StringUtils.isNotEmpty(type)) {
			searchRequest.types(type.split(","));
		}

		searchRequest.source(commonSearchSourceBuilderWithRetSizeLimit(startTime, endTime, size, fields, sortFieldList, matchPhrase, highlightFieldList, matchStr));

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		log.info("【列表查询】数据总数:{},处理总数:{}", searchResponse.getHits().totalHits, searchResponse.getHits().getHits().length);

		return highLightHandler(searchResponse);
	}

	/**
	 * 使用分词查询,并分页
	 *
	 * @param index              索引名称
	 * @param type               类型名称,可传入多个type逗号分隔
	 * @param currentPage        当前页
	 * @param pageSize           每页显示条数
	 * @param startTime          开始时间
	 * @param endTime            结束时间
	 * @param fields             需要显示的字段，逗号分隔（缺省为全部字段）
	 * @param sortFieldList      排序字段
	 * @param matchPhrase        true 使用，短语精准匹配
	 * @param highlightFieldList 高亮字段
	 * @param matchStr           过滤条件（xxx=111,aaa=222）
	 * @return
	 */
	public static EsPage searchDataPage(String index, String type, int currentPage, int pageSize, long startTime, long endTime, String fields, List<Map<String, String>> sortFieldList, boolean matchPhrase, List<String> highlightFieldList, String matchStr) throws Exception {
		SearchRequest searchRequest = new SearchRequest(index);
		if (StringUtils.isNotEmpty(type)) {
			searchRequest.types(type.split(","));
		}

		searchRequest.source(commonSearchSourceBuilderWithPage(currentPage, pageSize, startTime, endTime, fields, sortFieldList, matchPhrase, highlightFieldList, matchStr));

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		long totalHits = searchResponse.getHits().totalHits;
		long length = searchResponse.getHits().getHits().length;

		log.info("【分页查询】数据总数:{},处理总数:{}", totalHits, length);

		List<Map<String, Object>> sourceList = highLightHandler(searchResponse);
		return new EsPage(currentPage, pageSize, (int) totalHits, sourceList);
	}

	/**
	 * 使用分词查询,并分页
	 *
	 * @param index              索引名称
	 * @param type               类型名称,可传入多个type逗号分隔
	 * @param currentPage        当前页
	 * @param pageSize           每页显示条数
	 * @param startTime          开始时间
	 * @param endTime            结束时间
	 * @param fields             需要显示的字段，逗号分隔（缺省为全部字段）
	 * @param sortFieldList      排序字段
	 * @param matchPhrase        true 使用，短语精准匹配
	 * @param highlightFieldList 高亮字段
	 * @param matchStr           过滤条件（xxx=111,aaa=222）
	 * @return
	 */
	public static EsPage searchDataPageGrouped(String index, String type, int currentPage, int pageSize, long startTime, long endTime, String fields, List<Map<String, String>> sortFieldList, boolean matchPhrase, List<String> highlightFieldList, String matchStr, String groupField) throws Exception {
		SearchRequest searchRequest = new SearchRequest(index);
		if (StringUtils.isNotEmpty(type)) {
			searchRequest.types(type.split(","));
		}

		searchRequest.source(
				groupCondition(
						commonSearchSourceBuilderWithPage(currentPage
								, pageSize
								, startTime
								, endTime
								, fields
								, sortFieldList
								, matchPhrase
								, highlightFieldList
								, matchStr)
						, groupField
				)
		);

		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		long totalHits = searchResponse.getHits().totalHits;
		long length = searchResponse.getHits().getHits().length;

		log.info("【分页聚合查询】数据总数:{},处理总数:{}", totalHits, length);

		List<Map<String, Object>> sourceList = highLightHandler(searchResponse);
		return new EsPage(currentPage, pageSize, (int) totalHits, sourceList);
	}

	/**
	 * 使用分词查询,并分页
	 *
	 * @param startIndex         当前页
	 * @param pageSize           每页显示条数
	 * @param startTime          开始时间
	 * @param endTime            结束时间
	 * @param fields             需要显示的字段，逗号分隔（缺省为全部字段）
	 * @param sortFieldList      排序字段
	 * @param matchPhrase        true 使用，短语精准匹配
	 * @param highlightFieldList 高亮字段
	 * @param matchStr           过滤条件（xxx=111,aaa=222）
	 * @return
	 */
	private static SearchSourceBuilder commonSearchSourceBuilderWithPage(int startIndex, int pageSize, long startTime, long endTime, String fields, List<Map<String, String>> sortFieldList, boolean matchPhrase, List<String> highlightFieldList, String matchStr) {
		SearchSourceBuilder searchSourceBuilder = commonSearchSourceBuilderBasic(startTime, endTime, fields, sortFieldList, matchPhrase, highlightFieldList, matchStr);
		searchSourceBuilder.from(pageSize * (startIndex - 1)).size(pageSize);
		return searchSourceBuilder;
	}

	/**
	 * 查询条件（基础条件）
	 *
	 * @param startTime          开始时间
	 * @param endTime            结束时间
	 * @param fields             需要显示的字段，逗号分隔（缺省为全部字段）
	 * @param sortFieldList      排序字段
	 * @param matchPhrase        true 使用，短语精准匹配
	 * @param highlightFieldList 高亮字段
	 * @param matchStr           过滤条件（xxx=111,aaa=222）
	 * @return
	 */
	private static SearchSourceBuilder commonSearchSourceBuilderBasic(long startTime, long endTime, String fields, List<Map<String, String>> sortFieldList, boolean matchPhrase, List<String> highlightFieldList, String matchStr) {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();

		/* 字段与时间限制 */
		searchSourceBuilder = rangeFilterCondition(searchSourceBuilder, startTime, endTime, matchStr, matchPhrase);

		/* 高亮 */
		if (null != highlightFieldList && !highlightFieldList.isEmpty()) {
			searchSourceBuilder = highLightCondition(searchSourceBuilder, highlightFieldList);
		}

		/* 字段 */
		if (StringUtils.isNotEmpty(fields)) {
			searchSourceBuilder.fetchSource(fields.split(","), null);
		}
		/* 是否带有源码 */
		searchSourceBuilder.fetchSource(true);

		/* 排序字段 */
		if (null != sortFieldList && sortFieldList.size() > 0) {
			for (Map<String, String> fieldInfo : sortFieldList) {
				searchSourceBuilder.sort(
						fieldInfo.get("field")
						, "asc".equals(fieldInfo.get("sort")) ? SortOrder.ASC : SortOrder.DESC);
			}
		}
		log.trace("【查询条件】Json:{}", JSONObject.toJSONString(searchSourceBuilder));

		return searchSourceBuilder;
	}

	/**
	 * 查询条件（限制结果大小）
	 *
	 * @param startTime          开始时间
	 * @param endTime            结束时间
	 * @param size               文档大小限制
	 * @param fields             需要显示的字段，逗号分隔（缺省为全部字段）
	 * @param sortFieldList      排序字段
	 * @param matchPhrase        true 使用，短语精准匹配
	 * @param highlightFieldList 高亮字段
	 * @param matchStr           过滤条件（xxx=111,aaa=222）
	 * @return
	 */
	private static SearchSourceBuilder commonSearchSourceBuilderWithRetSizeLimit(long startTime, long endTime, Integer size, String fields, List<Map<String, String>> sortFieldList, boolean matchPhrase, List<String> highlightFieldList, String matchStr) {
		SearchSourceBuilder searchSourceBuilder = commonSearchSourceBuilderBasic(startTime, endTime, fields, sortFieldList, matchPhrase, highlightFieldList, matchStr);
		if (size != null && size > 0) {
			searchSourceBuilder.size(size);
		}
		return searchSourceBuilder;
	}

	/**
	 * 高亮结果集 特殊处理
	 *
	 * @param searchResponse
	 */
	private static List<Map<String, Object>> highLightHandler(SearchResponse searchResponse) {
		List<Map<String, Object>> sourceList = new ArrayList<>();
		for (SearchHit searchHit : searchResponse.getHits().getHits()) {
			searchHit.getSourceAsMap().put("id", searchHit.getId());

			Map<String, HighlightField> highlightFieldMap = searchHit.getHighlightFields();
			if (null != highlightFieldMap && !highlightFieldMap.isEmpty()) {

				for (String highlightField : highlightFieldMap.keySet()) {
					Text[] text = searchHit.getHighlightFields().get(highlightField).getFragments();
					StringBuffer stringBuffer = new StringBuffer();
					if (text != null) {
						for (Text str : text) {
							stringBuffer.append(str.string());
						}
						//遍历 高亮结果集，覆盖 正常结果集
						searchHit.getSourceAsMap().put(highlightField, stringBuffer.toString());
					}
				}
			}
			sourceList.add(searchHit.getSourceAsMap());
		}
		return sourceList;
	}

	/**
	 * 查询字段与结果集时间范围
	 *
	 * @param startTime   开始时间
	 * @param endTime     结束时间
	 * @param matchPhrase true 使用，短语精准匹配
	 * @param matchStr    过滤条件（xxx=111,aaa=222）
	 * @return
	 */
	private static SearchSourceBuilder rangeFilterCondition(SearchSourceBuilder searchSourceBuilder, long startTime, long endTime, String matchStr, boolean matchPhrase) {
		BoolQueryBuilder boolQuery = QueryBuilders.boolQuery();
		if (startTime > 0 && endTime > 0) {
			boolQuery.must(QueryBuilders.rangeQuery("timestamp")
					.format("epoch_millis")
					.from(startTime)
					.to(endTime)
					.includeLower(true)
					.includeUpper(true)
			);
		}

		if (StringUtils.isNotEmpty(matchStr)) {
			for (String s : matchStr.split(",")) {
				String[] ss = s.split("=");
				if (ss.length > 1) {
					if (matchPhrase == Boolean.TRUE) {
						boolQuery.must(QueryBuilders.matchPhraseQuery(s.split("=")[0], s.split("=")[1]));
					} else {
						boolQuery.must(QueryBuilders.matchQuery(s.split("=")[0], s.split("=")[1]));
					}
				}

			}
		}
		return searchSourceBuilder.query(boolQuery);
	}

	/**
	 * 高亮处理
	 *
	 * @param searchSourceBuilder 原始条件
	 * @param highlightFieldList  高亮字段列表
	 * @return
	 */
	private static SearchSourceBuilder highLightCondition(SearchSourceBuilder searchSourceBuilder, List<String> highlightFieldList) {
		HighlightBuilder highlightBuilder = new HighlightBuilder();
		highlightBuilder.preTags("<span class='high-light'>");//设置前缀
		highlightBuilder.postTags("</span>");//设置后缀
		for (String field : highlightFieldList) {
			highlightBuilder.field(field);
		}
		return searchSourceBuilder.highlighter(highlightBuilder);
	}

	/**
	 * 根据id多选(TODO 这里可以优化,变更为通用方法)
	 *
	 * @param index  索引名称
	 * @param type   类型名称,可传入多个type逗号分隔
	 * @param idList id列表
	 * @return
	 * @throws IOException
	 */
	public static List<Map<String, Object>> findByIdList(String index, String type, List<String> idList) throws IOException {
		MultiGetRequest multiGetRequest = new MultiGetRequest();
		for (String id : idList) {
			multiGetRequest.add(index, type, id);
		}
		MultiGetResponse multiGetResponse = client.mget(multiGetRequest, RequestOptions.DEFAULT);
		List<Map<String, Object>> resultList = new ArrayList();
		for (MultiGetItemResponse item : multiGetResponse.getResponses()) {
			GetResponse getResponse = item.getResponse();

			if (getResponse.isExists()) {
				resultList.add(getResponse.getSourceAsMap());
			}
		}
		log.info("【批量Id查询】数据总数:{}", resultList.size());
		return resultList;
	}

	/**
	 * 聚合处理
	 *
	 * @param searchSourceBuilder 原始条件
	 * @param fieldName           字段名
	 * @return
	 */
	private static SearchSourceBuilder groupCondition(SearchSourceBuilder searchSourceBuilder, String fieldName) {
		AggregationBuilder aggregation = AggregationBuilders.terms(fieldName + "_count").field("fieldName");
		return searchSourceBuilder.aggregation(aggregation);
	}

	/**
	 * 生成uuid
	 *
	 * @return
	 */
	private static String uuid() {
		return "ES_" + UUID.randomUUID().toString().replaceAll("-", "").toUpperCase();
	}

}