package com.sky.signal.pre.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.pre.processor.workLiveProcess.LiveProcess;
import com.sky.signal.pre.processor.workLiveProcess.WorkLiveFilterProcess;
import com.sky.signal.pre.processor.workLiveProcess.WorkProcess;
import com.sky.signal.pre.service.ComputeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 过滤有效时段的有效数据，用来做职住分析
 */
@Service("workLiveFilterService")
public class WorkLiveFilterService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(WorkLiveFilterService.class);

    @Autowired
    protected WorkLiveFilterProcess workLiveFilterProcess;
    @Autowired
    private transient WorkProcess workProcess;
    @Autowired
    private transient LiveProcess liveProcess;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        workLiveFilterProcess.process();
        liveProcess.process();
        workProcess.process();
        logger.info("WorkLiveFilterService duration: " + stopwatch.toString());
    }
}