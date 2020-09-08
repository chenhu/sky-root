package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.processor.baseAnalyze.CellProcess;
import com.sky.signal.pre.service.ComputeService;
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
        cellProcess.process();
    }
}