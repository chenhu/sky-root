package com.sky.signal.pre.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.signalProcess.SignalProcessor;
import com.sky.signal.pre.service.ComputeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 原始信令数据预处理成有效信令数据
 */
@Service("signalProcessService")
public class SignalProcessService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(SignalProcessService.class);
    @Autowired
    private SignalProcessor signalProcessor;
    @Autowired
    private ParamProperties params;
    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        if(params.getRunMode().equals("province")) {
            signalProcessor.processProvince();
        }else if(params.getRunMode().equals("district")) {
            signalProcessor.processDistrict();
        }
        logger.info("SignalProcessService duration: " + stopwatch.toString());
    }
}