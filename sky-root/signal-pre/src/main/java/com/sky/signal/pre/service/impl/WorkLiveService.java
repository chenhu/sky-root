package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.processor.workLiveProcess.LiveWorkAggProcessor;
import com.sky.signal.pre.service.ComputeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 职住判断处理服务
 */
@Service("workLiveService")
public class WorkLiveService implements ComputeService {
    @Autowired
    private LiveWorkAggProcessor liveWorkAggProcessor;

    @Override
    public void compute() {
        liveWorkAggProcessor.process();
    }
}