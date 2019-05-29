package com.sky.signal.pre.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.dataquality.DataQualityProcessor;
import com.sky.signal.pre.service.ComputeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/5/13 17:22
 * description: 原始数据质量验证服务
 */
@Service("dataQualityService")
public class DataQualityService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(DataQualityService.class);
    @Autowired
    private transient ParamProperties params;

    @Autowired
    private transient DataQualityProcessor dataQualityProcessor;
    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        for (String traceSignalFile: params.getTraceSignalFileFullPath()) {
            dataQualityProcessor.process(traceSignalFile);
        }
        logger.info("DataQualityService duration: " + stopwatch.toString());

    }
}
