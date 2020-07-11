package com.sky.signal.population.service;

import com.google.common.base.Stopwatch;
import com.sky.signal.population.processor.CellProcess;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * 基站数据处理服务
 */
@Component("CellService")
public class CellService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(CellService.class);

    @Autowired
    private CellProcess cellProcess;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        cellProcess.process();
        logger.info("CellService duration: " + stopwatch.toString());
    }
}