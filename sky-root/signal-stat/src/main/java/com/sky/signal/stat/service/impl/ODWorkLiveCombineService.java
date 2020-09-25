package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.CombineODWorkLive;
import com.sky.signal.stat.processor.od.ODLoader;
import com.sky.signal.stat.processor.workLive.WorkLiveLoader;
import com.sky.signal.stat.service.ComputeService;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service("oDWorkLiveCombineService")
public class ODWorkLiveCombineService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(ODWorkLiveCombineService.class);
    @Autowired
    private transient CombineODWorkLive combineODWorkLive;
    @Autowired
    private transient ODLoader odLoader;
    @Autowired
    private transient WorkLiveLoader workLiveLoader;
    @Autowired
    private transient ParamProperties params;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();

        Map<Integer, List<String>> odMap = FilesBatchUtils.getBatchFiles(params.getODResultPath(), params.getStatBatchSize(),params.getCrashPosition());
        DataFrame workLiveDf = workLiveLoader.loadMergedWorkLive();
        workLiveDf.persist(StorageLevel.DISK_ONLY());
        // 分批次预处理
        for( int batchId: odMap.keySet()) {
            List<String> odFiles = odMap.get(batchId);
            DataFrame odDf = odLoader.loadOD(odFiles);
            combineODWorkLive.process(odDf, workLiveDf, batchId);
        }
        workLiveDf.unpersist();
        logger.info("ODWorkLiveCombineService duration: " + stopwatch.toString());
    }
}
