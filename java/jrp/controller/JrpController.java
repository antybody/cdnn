package com.baosight.iwater.gxsy.jrp.controller;

import com.baosight.iwater.gxsy.jrp.service.IJRPService;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;

/**
 *调用pyton算法
 */
@RestController
@RequestMapping("/gx/jrp")
public class JrpController {
	@Resource
	private IJRPService jrpService;

	@RequestMapping(value="/RunPy",produces = "text/html;charset=UTF-8")
	public String JrpResult(HttpServletRequest request) {
		return jrpService.jrpResult(request);
	}
}
