package com.xavier.ktl.api;

import com.xavier.ktl.service.QueryService;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/")
public class TestApi {

	@Autowired
	private QueryService queryService;

	@ApiOperation(value = "测试", httpMethod = "POST")
	@PostMapping("test")
	public String test(){
		try {
			queryService.dealData();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "Done";
	}
}
