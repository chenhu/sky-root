package com.sky.signal.pre.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.workLiveProcess.ExistsDayProcess;
import com.sky.signal.pre.service.ComputeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 分批次预处理出现天数
 */
@Service
public class WorkLiveExistsDayService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(WorkLiveExistsDayService.class);
    @Autowired
    private transient ExistsDayProcess existsDayProcess;
    @Autowired
    private transient ParamProperties params;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<Integer, List<String>> liveValidSignalFileMap = SelectSignalFilesByBatch.getBatchFiles(params.getValidSignalFilesForWorkLive(), params.getWorkliveBatchSize());
        // 出现天数分批次预处理
        for( int batchId: liveValidSignalFileMap.keySet()) {
            List<String> validSignalFiles = liveValidSignalFileMap.get(batchId);
            existsDayProcess.process(SelectSignalFilesByBatch.getValidSignal(validSignalFiles), batchId);
        }
        logger.info("WorkLiveExistsDayService duration: " + stopwatch.toString());
    }

}