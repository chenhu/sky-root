package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.signalProcess.SignalProcessor;
import com.sky.signal.pre.service.ComputeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * 原始信令数据预处理成有效信令数据
 */
@Service
public class SignalProcessService implements ComputeService {
    @Autowired
    private SignalProcessor signalProcessor;
    @Autowired
    private ParamProperties params;
    @Override
    public void compute() {
        signalProcessor.process();
    }
}