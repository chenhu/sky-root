package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.processor.crmAnalyze.CRMProcess;
import com.sky.signal.pre.service.ComputeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * crm信息预处理服务，注意是缩小提供的crm文件大小，去除重复的crm用户信息
 */
@Component("CrmPreService")
public class CrmPreService implements ComputeService {
    @Autowired
    private CRMProcess crmProcess;
    @Override
    public void compute() {
        crmProcess.process();
    }
}