package com.xavier.ktl;

import com.xavier.es.util.ElasticsearchUtil;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RunWith(SpringRunner.class)
@SpringBootTest
public class KtlApplicationTests {

	@Autowired
	private JdbcTemplate jdbcTemplate;

	@Test
	public void queryTest() {
		int limit = 100;
		int offset = 0;
		String sql = "select * from pt_petition_case limit " + limit + " offset " + offset;
		List<Map<String, Object>> list = jdbcTemplate.queryForList(sql);
		list.stream().forEach(System.out::println);
	}

	@Test
	public void threadTest() {
		Runnable stateChangeThreadOffset0 = () ->
				System.out.println(jdbcTemplate.queryForList("select * from pt_petition_case limit 100 offset 0").get(0).get("id"));

		Runnable stateChangeThreadOffset100 = () ->
				System.out.println(jdbcTemplate.queryForList("select * from pt_petition_case limit 100 offset 100").get(0).get("id"));

		Runnable stateChangeThreadOffset200 = () ->
				System.out.println(jdbcTemplate.queryForList("select * from pt_petition_case limit 100 offset 200").get(0).get("id"));

		stateChangeThreadOffset0.run();
		stateChangeThreadOffset100.run();
		stateChangeThreadOffset200.run();
	}

	@Test
	public void configTest() throws Exception {
		Map<String, Object> map = new HashMap<>();
		map.put("time", new Date());
		map.put("test", "fd4s65af4d5sfa65");
		ElasticsearchUtil.addData(map, "pt_petition_high_level_reduce", "pt_petition_high_level_reduce");
	}
}
