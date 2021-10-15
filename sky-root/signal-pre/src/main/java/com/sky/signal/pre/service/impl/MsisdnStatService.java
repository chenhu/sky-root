package com.sky.signal.pre.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.district.DistrictMsisdnStatProcessor;
import com.sky.signal.pre.service.ComputeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 手机号码统计，临时用于统计
 */
@Service("msisdnStatService")
public class MsisdnStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(MsisdnStatService.class);
    @Autowired
    private DistrictMsisdnStatProcessor districtMsisdnStatProcessor;
    @Autowired
    private ParamProperties params;
    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        districtMsisdnStatProcessor.process();
        logger.info("SignalProcessService duration: " + stopwatch);
    }
}