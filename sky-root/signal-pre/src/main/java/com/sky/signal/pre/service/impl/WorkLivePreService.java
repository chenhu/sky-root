package com.sky.signal.pre.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.workLiveProcess.LiveProcess;
import com.sky.signal.pre.processor.workLiveProcess.WorkProcess;
import com.sky.signal.pre.service.ComputeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 职住分析分批次预处理
 */
@Service("workLivePreService")
public class WorkLivePreService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(WorkLivePreService.class);
    @Autowired
    private transient WorkProcess workProcess;
    @Autowired
    private transient LiveProcess liveProcess;
    @Autowired
    private transient ParamProperties params;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<Integer, List<String>> liveValidSignalFileMap = SelectSignalFilesByBatch.getBatchFiles(params.getValidSignalFilesForWorkLive(), params.getWorkliveBatchSize());
        Map<Integer, List<String>> workValidSignalFileMap = SelectSignalFilesByBatch.getBatchFiles(params.getValidSignalFilesForWorkLive(), params.getWorkliveBatchSize());

        // 居住地分批次预处理
        for( int batchId: liveValidSignalFileMap.keySet()) {
            List<String> validSignalFiles = liveValidSignalFileMap.get(batchId);
            liveProcess.process(SelectSignalFilesByBatch.getValidSignal(validSignalFiles), batchId);
        }
        // 工作地分批次预处理
        for( int batchId: workValidSignalFileMap.keySet()) {
            List<String> validSignalFiles = workValidSignalFileMap.get(batchId);
            workProcess.process(SelectSignalFilesByBatch.getValidSignal(validSignalFiles), batchId);
        }
        logger.info("WorkLivePreService duration: " + stopwatch.toString());
    }

}