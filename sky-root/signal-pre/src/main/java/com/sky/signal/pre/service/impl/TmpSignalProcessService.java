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
 * 省级别的预处理是按照地市进行的，为了排除一些问题，现在需要对预处理后的数据，再进行一次预处理，一次处理一天全省数据
 */
@Service
public class TmpSignalProcessService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(TmpSignalProcessService.class);
    @Autowired
    private SignalProcessor signalProcessor;
    @Autowired
    private ParamProperties params;
    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        if(params.getRunMode().equals("province")) {
            signalProcessor.tmpProcessProvince();
        }
        logger.info("SignalProcessService duration: " + stopwatch.toString());
    }
}