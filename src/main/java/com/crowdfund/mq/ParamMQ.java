package com.crowdfund.mq;

import java.util.Map;

public class ParamMQ{

	/*
	 * 业务类型code
	 */
	private String bizCode;
	
	/*
	 * 业务信息
	 */
	private String bizMsg;
	
	/*
	 * 存放参数对象
	 */
	private Map<String, Object> params;

	public String getBizCode() {
		return bizCode;
	}

	public void setBizCode(String bizCode) {
		this.bizCode = bizCode;
	}

	public String getBizMsg() {
		return bizMsg;
	}

	public void setBizMsg(String bizMsg) {
		this.bizMsg = bizMsg;
	}

	public Map<String, Object> getParams() {
		return params;
	}

	public void setParams(Map<String, Object> params) {
		this.params = params;
	}
	
}
