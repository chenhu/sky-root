package com.sky.signal.pre.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.pre.processor.crmAnalyze.CRMProcess;
import com.sky.signal.pre.service.ComputeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * crm信息预处理服务，注意是缩小提供的crm文件大小，去除重复的crm用户信息
 */
@Component("CrmPreService")
public class CrmPreService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(CrmPreService.class);

    @Autowired
    private CRMProcess crmProcess;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        crmProcess.process();
        logger.info("CellService duration: " + stopwatch.toString());
    }
}