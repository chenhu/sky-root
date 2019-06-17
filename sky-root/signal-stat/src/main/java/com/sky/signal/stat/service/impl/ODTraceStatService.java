package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.ODTraceStat;
import com.sky.signal.stat.processor.od.ODLoader;
import com.sky.signal.stat.processor.workLive.WorkLiveLoader;
import com.sky.signal.stat.service.ComputeService;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service("oDTraceStatService")
public class ODTraceStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(ODTraceStatService.class);
    @Autowired
    private transient ODLoader odLoader;
    @Autowired
    private transient WorkLiveLoader workLiveLoader;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient ODTraceStat odTraceStat;
    @Autowired
    private transient SQLContext sqlContext;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        Map<Integer, List<String>> odTraceMap = FilesBatchUtils.getBatchFiles(params.getOdTraceFiles(), params.getStatBatchSize());
        DataFrame workLiveDf = workLiveLoader.load(params.getWorkLiveFile());
        workLiveDf.persist(StorageLevel.DISK_ONLY());
        // 分批次预处理
        for( int batchId: odTraceMap.keySet()) {
            List<String> odTraceFiles = odTraceMap.get(batchId);
            DataFrame odTraceDf = odLoader.loadODTrace(odTraceFiles);
            odTraceStat.process(odTraceDf, workLiveDf,batchId, sqlContext);
        }
        workLiveDf.unpersist();
        odTraceStat.combineData();

        logger.info("ODTraceStatService duration: " + stopwatch.toString());
    }
}
