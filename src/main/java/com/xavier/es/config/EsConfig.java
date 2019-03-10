package com.xavier.es.config;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.io.IOException;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Order(Ordered.HIGHEST_PRECEDENCE)
@Component
@ConfigurationProperties(prefix = "sync.elasticsearch")
public class EsConfig {

	private String host = "127.0.0.1";
	private int port = 9200;
	private String schema = null;
	private int connectTimeOut = 100000;
	private int socketTimeOut = 3000000;
	private int connectionRequestTimeOut = 50000;

	private int maxConnectNum = 100;
	private int maxConnectPerRoute = 100;

	private HttpHost httpHost = null == schema ? new HttpHost(host, port) : new HttpHost(host, port, schema);
	private boolean uniqueConnectTimeConfig = true;
	private boolean uniqueConnectNumConfig = true;
	private RestClientBuilder builder;
	private RestHighLevelClient client;

	@Bean
	public RestHighLevelClient client() {
		builder = RestClient.builder(httpHost);
		if (uniqueConnectTimeConfig) {
			setConnectTimeOutConfig();
		}
		if (uniqueConnectNumConfig) {
			setMutiConnectConfig();
		}
		client = new RestHighLevelClient(builder);
		return client;
	}

	// 主要关于异步httpclient的连接延时配置
	public void setConnectTimeOutConfig() {
		builder.setRequestConfigCallback(
				requestConfigBuilder -> {
					requestConfigBuilder.setConnectTimeout(connectTimeOut);
					requestConfigBuilder.setSocketTimeout(socketTimeOut);
					requestConfigBuilder.setConnectionRequestTimeout(connectionRequestTimeOut);
					return requestConfigBuilder;
				}
		);
	}

	// 主要关于异步httpclient的连接数配置
	public void setMutiConnectConfig() {
		builder.setHttpClientConfigCallback(
				(httpClientBuilder) -> {
					httpClientBuilder.setMaxConnTotal(maxConnectNum);
					httpClientBuilder.setMaxConnPerRoute(maxConnectPerRoute);
					return httpClientBuilder;
				}
		);
	}

	public void close() {
		if (client != null) {
			try {
				client.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
