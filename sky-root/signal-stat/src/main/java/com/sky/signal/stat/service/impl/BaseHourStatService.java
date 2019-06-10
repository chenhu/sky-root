package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.BaseHourStat;
import com.sky.signal.stat.processor.BaseHourStatAgg;
import com.sky.signal.stat.processor.signal.SignalSchemaProvider;
import com.sky.signal.stat.processor.workLive.WorkLiveLoader;
import com.sky.signal.stat.service.ComputeService;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Service("baseHourStatService")
public class BaseHourStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(BaseHourStatService.class);
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient WorkLiveLoader workLiveLoader;
    @Autowired
    private transient BaseHourStat baseHourStat;
    @Autowired
    private transient BaseHourStatAgg baseHourStatAgg;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DataFrame workLiveDf = workLiveLoader.load(params.getWorkLiveFile());
        workLiveDf = workLiveDf.persist(StorageLevel.DISK_ONLY());
        Map<Integer, List<String>> workValidSignalFileMap = getBatchFiles(params.getValidSignalFilesForStat());
        for( int batchId: workValidSignalFileMap.keySet()) {
            List<String> validSignalFiles = workValidSignalFileMap.get(batchId);
            baseHourStat.process(getValidSignal(validSignalFiles),workLiveDf, batchId);
        }
        baseHourStatAgg.process();
        workLiveDf.unpersist();
        logger.info("BaseHourStatService duration: " + stopwatch.toString());
    }

    private Map<Integer, List<String>> getBatchFiles(List<String> fileList) {
        int workLiveBatchs = params.getStatBatchSize();
        List<String> batchValidSignalList = new ArrayList<>();
        Map<Integer, List<String>> batchMap = new HashMap<>();
        int batchNum = 1;
        for(int i=1; i <= fileList.size(); i++) {
            batchValidSignalList.add(fileList.get(i - 1));
            if(i% workLiveBatchs == 0 || i == fileList.size()) {
                batchMap.put(batchNum, batchValidSignalList);
                batchNum ++ ;
                batchValidSignalList = new ArrayList<>() ;
                continue;
            }
        }
        return batchMap;
    }

    private DataFrame getValidSignal(List<String> validSignalFiles) {
        DataFrame validSignalDF = null;
        for (String ValidSignalFile : validSignalFiles) {
            DataFrame validDF = FileUtil.readFile(FileUtil.FileType.CSV, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA, ValidSignalFile);
            if (validSignalDF == null) {
                validSignalDF = validDF;
            } else {
                validSignalDF = validSignalDF.unionAll(validDF);
            }
        }

        return validSignalDF;
    }
}
