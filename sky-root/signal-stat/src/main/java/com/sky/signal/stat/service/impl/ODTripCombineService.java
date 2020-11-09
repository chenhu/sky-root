package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.CombineTripStatWorkLive;
import com.sky.signal.stat.processor.od.ODLoader;
import com.sky.signal.stat.processor.workLive.WorkLiveLoader;
import com.sky.signal.stat.service.ComputeService;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service("oDTripCombineService")
public class ODTripCombineService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(ODTripCombineService.class);
    @Autowired
    private transient CombineTripStatWorkLive combineTripStatWorkLive;
    @Autowired
    private transient ODLoader odLoader;
    @Autowired
    private transient WorkLiveLoader workLiveLoader;
    @Autowired
    private transient ParamProperties params;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        if(params.getCrashPosition() < 2) {
            this.clearOdTripTempData();
        }
        Map<Integer, List<String>> odTripMap = FilesBatchUtils.getBatchFiles(params.getODStatTripPath(), params.getStatBatchSize(),params.getCrashPosition());
        DataFrame workLiveDf = workLiveLoader.loadMergedWorkLive().repartition(params.getPartitions());
        workLiveDf.persist(StorageLevel.DISK_ONLY());
        // 分批次预处理
        for( int batchId: odTripMap.keySet()) {
            List<String> odTripFiles = odTripMap.get(batchId);
            DataFrame odTripDf = odLoader.loadODTripStat(odTripFiles);
            combineTripStatWorkLive.process(odTripDf, workLiveDf, batchId);
        }
        workLiveDf.unpersist();
        logger.info("ODTripCombineService duration: " + stopwatch.toString());
    }

    private void clearOdTripTempData() {
        FileUtil.removeDfsDirectory(params.getODTripStatTempSavePath());
    }
}
