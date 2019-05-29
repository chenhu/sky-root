package com.sky.signal.pre.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.pre.processor.workLiveProcess.LiveWorkAggProcessor;
import com.sky.signal.pre.service.ComputeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 职住判断处理服务
 */
@Service("workLiveService")
public class WorkLiveService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(WorkLiveService.class);

    @Autowired
    private LiveWorkAggProcessor liveWorkAggProcessor;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        liveWorkAggProcessor.process();
        logger.info("WorkLiveService duration: " + stopwatch.toString());
    }
}