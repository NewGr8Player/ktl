package com.xavier.ktl.bean;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

/**
 * 用于生成查询sql
 *
 * @author NewGr8Player
 */

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
@ConfigurationProperties(prefix = "sync.query")
public class QuerySql {

	/**
	 * 最大分页大小
	 */
	private int maxPageNum = 5;


	/**
	 * 映射关系
	 */
	private List<QueryInfo> sql = new ArrayList<>();

	@Getter
	@Setter
	@NoArgsConstructor
	@AllArgsConstructor
	public static class QueryInfo {

		/**
		 * 索引名称
		 */
		private String indexName = "";

		/**
		 * 类型名称
		 */
		private String typeName = "";

		/**
		 * 数据查询sql
		 */
		private String querySql = "";

		/**
		 * 计数sql
		 */
		private String countSql = "";

		@Override
		public String toString() {
			return new StringJoiner(", ", QueryInfo.class.getSimpleName() + "[", "]")
					.add("indexName='" + indexName + "'")
					.add("typeName='" + typeName + "'")
					.add("querySql='" + querySql + "'")
					.add("countSql='" + countSql + "'")
					.toString();
		}
	}
}
