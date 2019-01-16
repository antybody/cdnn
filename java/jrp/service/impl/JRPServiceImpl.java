package com.baosight.iwater.gxsy.jrp.service.impl;

import com.alibaba.fastjson.JSON;
import com.baosight.iwater.define.PythonUtil;
import com.baosight.iwater.gxsy.jrp.pojo.Pjrp;
import com.baosight.iwater.gxsy.jrp.service.IJRPService;
import com.baosight.iwater.system.db.manager.PropertiesManager;
import com.baosight.iwater.system.define.AppInfo;
import com.baosight.iwater.system.define.State;
import com.baosight.iwater.system.define.impl.BaseState;

import org.springframework.stereotype.Service;

import javax.servlet.http.HttpServletRequest;

import java.net.URLDecoder;
import java.util.Map;

@Service("jrpService")
public class JRPServiceImpl implements IJRPService {

    @Override
    public String jrpResult(HttpServletRequest request) {
    	
        State state=new BaseState();
        try{
            //获得传入数据  转换为map
            String stk=URLDecoder.decode(request.getParameter("params"),"UTF-8");
            Pjrp pjrp=JSON.parseObject(stk,Pjrp.class);
            if(pjrp==null || pjrp.getStartDate()==null){
                return state.addState(AppInfo.NO_PARAM,null);  //传入数据格式不正确
            }
            if(pjrp==null || pjrp.getEndDate()==null){
                return state.addState(AppInfo.NO_PARAM,null);  //传入数据格式不正确
            }
            if(pjrp==null || pjrp.getArtis()==null){
                return state.addState(AppInfo.NO_PARAM,null);  //传入数据格式不正确
            }
            if(pjrp==null || pjrp.getDeal()==null){
                return state.addState(AppInfo.NO_PARAM,null);  //传入数据格式不正确
            }
            String  filePath="";
			
            String[] args1 = new String[] {"python",filePath ,pjrp.getStartDate(),pjrp.getEndDate(),pjrp.getArtis(),pjrp.getDeal()};
            String result = PythonUtil.execute(args1, new String[]{"gbk"});
            
            return state.addState(AppInfo.SUCCESS, result);
        }
        catch(Exception e){
            e.printStackTrace();
            return state.addState(AppInfo.NO_PARAM,null);   //传入数据格式不正确
        }
    }


    public static void main(String[] args) {
        String[] args1 = new String[] { "python", "D:\\workroot\\worktools\\git\\GitHouse\\run.py","0","2017-01-01","2017-12-30"};
        String result = PythonUtil.execute(args1, new String[]{"gbk"});
        System.out.println(result);
    }
}
