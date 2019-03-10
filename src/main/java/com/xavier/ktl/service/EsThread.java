package com.xavier.ktl.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.xavier.es.util.ElasticsearchUtil;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
public class EsThread {

	private static final Map<String, Object> protypeMap = new HashMap<>();

	static {
		protypeMap.put("id", null);//数据id，基础规则： 信访件id_表名_本表id
		protypeMap.put("data_source", null);//数据来源 sjly
		protypeMap.put("create_by", null);//创建人
		protypeMap.put("create_date", null);//创建时间
		protypeMap.put("update_by", null);//更新者
		protypeMap.put("update_date", null);//更新时间

		protypeMap.put("petition_case_id", null);//信访件id xfj_id
		protypeMap.put("petition_case_no", null);//信访件编号 xfjbh
		protypeMap.put("addressee_man", null);//受信人 sxr
		protypeMap.put("attribution_area_code", null);//问题属地代码 wtsddm
		protypeMap.put("attribution_area_detail", null);//问题属地（完整）
		protypeMap.put("belong_system_code", null);//所属系统 ssxt
		protypeMap.put("cognizance_flag", null);//审核认定办结标志 shrdbjbz
		protypeMap.put("content_type_code", null);//内容分类代码 nrfldm
		protypeMap.put("content_type_label", null);//内容分类文本
		protypeMap.put("crowd_flag", null);//集体访标识 (1:集体访)
		protypeMap.put("deadline_date", null);//限办截止时间 xbjzrq
		protypeMap.put("department_id", null);//归属部门
		protypeMap.put("evaluate_flag", null);//参与评价
		protypeMap.put("finish_flag", null);//办结标志 bjbz
		protypeMap.put("first_date", null);//首次信访日期 scxfrq
		protypeMap.put("first_office_name", null);//首次信访机构 scxfjg
		protypeMap.put("hot_spot_code", null);//热点问题 rdwt
		protypeMap.put("involve_man_amount", null);//涉及人数 sjrs
		protypeMap.put("letter_id", null);//信件条码 letterid
		protypeMap.put("no", null);//信访件编号 xfjbh
		protypeMap.put("office_id", null);//归属机构
		protypeMap.put("petition_date", null);//信访日期 xfrq
		protypeMap.put("petition_man_amount", null);//信访人数 xfrs
		protypeMap.put("petition_status_code", null);//信访件状态 xfjzt
		protypeMap.put("petition_type_code", null);//信访形式 xfxs
		protypeMap.put("purpose_code", null);//信访目的代码 xfmddm
		protypeMap.put("query_code", null);//查询码
		protypeMap.put("reason_code", null);//信访原因代码 xfyydm
		protypeMap.put("recheck_flag", null);//复核标志 fhbz
		protypeMap.put("reflected_area_code", null);//被反映人住址代码 bfyrzzdm
		protypeMap.put("reflected_area_name", null);//被反映人住址名称 bfyrzzmc
		protypeMap.put("reflected_duty", null);//被反映人职务 bfyrzw
		protypeMap.put("reflected_grade_code", null);//被反映人级别 bfyrjb
		protypeMap.put("reflected_object", null);//被反映人或单位 bfyrhdw
		protypeMap.put("register_date", null);//登记时间 djsj
		protypeMap.put("register_department_id", null);//登记部门 djbm
		protypeMap.put("register_department_name", null);//登记部门 djbm
		protypeMap.put("register_man", null);//登记人 djr
		protypeMap.put("register_office_category", null);//登记机构类别 djjglb
		protypeMap.put("register_office_code", null);//登记机构代码 djjgdm
		protypeMap.put("register_office_name", null);//登记机构名称 djjgmc
		protypeMap.put("remarks", null);//备注信息
		protypeMap.put("repeat_no", null);//重复信访件编号 repeated_id
		protypeMap.put("review_flag", null);//复查标志 fcbz
		protypeMap.put("sort", null);//排序
		protypeMap.put("source_code", null);//信访来源代码
		protypeMap.put("special_flag", null);//是否特殊人员（0：否，1：是） tsry
		protypeMap.put("special_reason", null);//特殊原因
		protypeMap.put("supervision_flag", null);//是否转督办 额外附件 dbbz
		protypeMap.put("transact_date", null);//办理时间
		protypeMap.put("transact_department_id", null);//办理处室id
		protypeMap.put("transact_department_name", null);//办理处室名称
		protypeMap.put("transact_office_id", null);//办理机构id
		protypeMap.put("transact_office_name", null);//办理机构名称
		protypeMap.put("transact_way_code", null);//办理方式
		protypeMap.put("transact_way_label", null);//办理方式
		protypeMap.put("account_location", null);//户口所在地 hkszd
		protypeMap.put("address_code", null);//住址代码 zzdm
		protypeMap.put("address_label", null);//信访人住址 zz
		protypeMap.put("birth_date", null);//出生日期 csrq
		protypeMap.put("card_code", null);//证件类型 zjlx
		protypeMap.put("card_no", null);//证件号码 zjhm
		protypeMap.put("career_code", null);//职业代码  zy
		protypeMap.put("email", null);//电子邮件 dzyj
		protypeMap.put("fixed_telephone", null);//固定电话 gddh
		protypeMap.put("identify_code", null);//信访人国籍或港澳台居民身份代码  xfrgjhgatjmsf
		protypeMap.put("mailing_address", null);//通讯地址 txdz
		protypeMap.put("main_flag", null);//是否是主访人(0:否;1:是) zfrbz
		protypeMap.put("name", null);//信访人姓名
		protypeMap.put("nation_code", null);//民族代码 mz
		protypeMap.put("political_code", null);//政治面貌
		protypeMap.put("sex_code", null);//性别代码（0：女，1：男）
		protypeMap.put("telephone", null);//手机号 sjh
		protypeMap.put("usual_address", null);//常用住址
		protypeMap.put("work_office_name", null);//工作单位 gzdw
		protypeMap.put("zip_code", null);//邮政编码 yzbm
		protypeMap.put("anonymous_flag", null);//匿名标志 nmbz
		protypeMap.put("aomen_flag", null);//涉澳标志 sabz
		protypeMap.put("direct_flag", null);//中省直标志
		protypeMap.put("foreign_flag", null);//涉外标志 swbz
		protypeMap.put("gov_flag", null);//党委政府标志
		protypeMap.put("group_flag", null);//群体意见标志
		protypeMap.put("hongkong_flag", null);//涉港标志 sgbz
		protypeMap.put("joint_flag", null);//是否联名 lmbz
		protypeMap.put("law_in_flag", null);//是否依法逐级走访 sfyfzjzf
		protypeMap.put("local_first_flag", null);//本机构初次标志 bjgccbz
		protypeMap.put("national_first_flag", null);//全国初次标志 qgccbz
		protypeMap.put("openable_net_flag", null);//互联网公开 gkbz
		protypeMap.put("openable_person_flag", null);//信访人公开
		protypeMap.put("overseas_flag", null);//涉侨标志 sqbz
		protypeMap.put("overstock_flag", null);//积案标志 jabz
		protypeMap.put("repeat_flag", null);//重复信访标志 cfxfbz
		protypeMap.put("sue_flag", null);//涉法涉诉标志 sfssbz
		protypeMap.put("taiwan_flag", null);//涉台标志 stbz
		protypeMap.put("threat_flag", null);//是否扬言 yybz
		protypeMap.put("three_flag", null);//三跨三分离标志 sksflbz
		protypeMap.put("urgent_flag", null);//重大紧急标志 zdjjbz
		protypeMap.put("opinion_content", null);//意见书内容（yjsnr）
		protypeMap.put("petition_man_opinion_code", null);//信访人意见（0:同意1:不同意9:无明确意见）（xfryj）
		protypeMap.put("publish_flag", null);//发布状态
		protypeMap.put("send_date", null);//送达时间（qsbz）
		protypeMap.put("send_man", null);//送达人（sdr）
		protypeMap.put("send_man_phone", null);//送达人电话
		protypeMap.put("send_way_code", null);//送达方式（0:当面送达1:邮寄送达2:其他）（sdfs）
		protypeMap.put("sign_flag", null);//签收标志（0:拒收1:签收）
		protypeMap.put("transact_man", null);//办理人员
		protypeMap.put("transact_office_category", null);//办理机构类别（bljglb）
		protypeMap.put("transact_office_code", null);//办理机构代码（bljgdm）
		protypeMap.put("transact_way_id", null);//办理方式id
		protypeMap.put("duty_office_evaluate_date", null);//责任单位评价时间（zrdwpjsj）
		protypeMap.put("duty_office_evaluate_flag", null);//责任单位是否可以评价(0为否，1为是)
		protypeMap.put("duty_office_evaluate_status", null);//责任代为评价状态（0:待评价1:未评价2:已评价3：超期未评价）（zrdwpjzt）
		protypeMap.put("petition_office_evaluate_date", null);//信访部门评价时间（xfbmpjsj）
		protypeMap.put("petition_office_evaluate_flag", null);//信访部门是否可以评价(0为否，1为是)
		protypeMap.put("petition_office_evaluate_status", null);//信访部门评价状态（0:待评价1:未评价2:已评价3：超期未评价）（xfbmpjzt）
		protypeMap.put("to_duty_office_evaluate_code", null);//对责任单位评价满意值（0:满意1:基本满意3:不满意）（dzrdwpj）
		protypeMap.put("to_duty_office_evaluate_content", null);//对责任单位评价内容（dzrdwnr）
		protypeMap.put("to_duty_office_evaluate_reason", null);//对责任单位评价不满意原因
		protypeMap.put("to_petition_office_evaluate_code", null);//对信访部门评价满意值（0:满意1:基本满意3:不满意）（dxfbmpj）
		protypeMap.put("to_petition_office_evaluate_content", null);//对信访部门评价内容（dxfbmnr）
		protypeMap.put("to_petition_office_evaluate_reason", null);//对信访部门评价不满意原因
		protypeMap.put("copy_office_ids", null);//抄送机构ids(多个)
		protypeMap.put("copy_office_name", null);//抄送机构(多个)
		protypeMap.put("little_to_name", null);//小去向名称（xqxmc）
		protypeMap.put("reply_notify_content", null);//回复告知内容（hfgznr）
		protypeMap.put("to_office_code", null);//去向机构代码（qxjgdm）
		protypeMap.put("to_office_id", null);//去向机构id
		protypeMap.put("to_office_name", null);//去向机构（qxjg）
		protypeMap.put("transact_men", null);//经办人（jbr）
		protypeMap.put("transact_men_id", null);//办理人id
		protypeMap.put("transact_opinion", null);//办理意见（blyj）

	}

	/**
	 * 多线程入库数据
	 *
	 * @param list
	 * @param reduceTableName
	 * @param reduceTypeName
	 * @throws InterruptedException
	 * @throws ExecutionException
	 */
	public static void test(List<Map<String, Object>> list, final String reduceTableName, final String reduceTypeName) throws InterruptedException, ExecutionException {
		log.info("待入库资源数量：{}", list.size());
		int threadSize = 1000;
		int dataSize = list.size();
		int threadNum = dataSize / threadSize + 1;
		boolean special = dataSize % threadSize == 0;

		ExecutorService exec = Executors.newFixedThreadPool(threadNum);
		List<Callable<Integer>> tasks = new ArrayList<Callable<Integer>>();
		Callable<Integer> task;
		List<Map<String, Object>> cutList;

		for (int i = 0; i < threadNum; i++) {
			if (i == threadNum - 1) {
				if (special) {
					break;
				}
				cutList = list.subList(threadSize * i, dataSize);
			} else {
				cutList = list.subList(threadSize * i, threadSize * (i + 1));
			}

			final List<Map<String, Object>> listStr = cutList;
			task = () -> {
				listStr.stream().forEach(
						e -> {
							String reduceId = e.get("petition_case_id") + "_" + e.get("id");
							try {
								for(String key : e.keySet()){
									if(key.contains("_date")){
										e.put(key,(Date)e.get(key));
									}
								}
								log.debug(JSONObject.toJSONString(e));
								ElasticsearchUtil.updateDataById(JSONObject.parseObject(JSON.toJSONString(e)), reduceTableName, reduceTypeName, reduceId);
							} catch (IOException e1) {
								e1.printStackTrace();
								log.error(reduceId);
							}
						}
				);

				return 1;
			};
			tasks.add(task);
		}

		exec.invokeAll(tasks);
		// 关闭线程池
		exec.shutdown();
	}
}
