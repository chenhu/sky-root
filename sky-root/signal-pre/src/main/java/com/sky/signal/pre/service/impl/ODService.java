package com.sky.signal.pre.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.odAnalyze.StayPointProcessor;
import com.sky.signal.pre.service.ComputeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/3/20 14:37
 * description: 根据有效数据，按天生成用户OD信息
 */
@Service("odService")
public class ODService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(SignalProcessService.class);
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient StayPointProcessor stayPointProcessor;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        if (params.getRunMode().equals("common")) {
            for (String validSignalFile : params.getValidSignalListByDays()) {
                stayPointProcessor.process(validSignalFile);
            }
        } else if (params.getRunMode().equals("district")) {
            for (String validSignalFile : params.getValidSignalListByDays(params.getDistrictCode().toString())) {
                stayPointProcessor.process(validSignalFile);
            }
        } else if(params.getRunMode().equals("province")) {
            for (String validSignalFile : params.getValidSignalListByDays()) {
                stayPointProcessor.process(validSignalFile);
            }
        }
        logger.info("ODService duration: " + stopwatch.toString());

    }
}
