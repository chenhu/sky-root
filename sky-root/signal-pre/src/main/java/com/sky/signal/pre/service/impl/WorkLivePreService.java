package com.sky.signal.pre.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.signalProcess.SignalSchemaProvider;
import com.sky.signal.pre.processor.workLiveProcess.LiveProcess;
import com.sky.signal.pre.processor.workLiveProcess.WorkProcess;
import com.sky.signal.pre.service.ComputeService;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
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
        Map<Integer, List<String>> liveValidSignalFileMap = getBatchFiles(params.getValidSignalForLive());
        Map<Integer, List<String>> workValidSignalFileMap = getBatchFiles(params.getValidSignalForWork());

        // 居住地分批次预处理
        for( int batchId: liveValidSignalFileMap.keySet()) {
            List<String> validSignalFiles = liveValidSignalFileMap.get(batchId);
            liveProcess.process(getValidSignal(validSignalFiles), batchId);
        }
        // 工作地分批次预处理
        for( int batchId: workValidSignalFileMap.keySet()) {
            List<String> validSignalFiles = workValidSignalFileMap.get(batchId);
            workProcess.process(getValidSignal(validSignalFiles), batchId);
        }
        logger.info("WorkLiveFilterService duration: " + stopwatch.toString());
    }

    private Map<Integer, List<String>> getBatchFiles(List<String> fileList) {
        int workLiveBatchs = params.getWorkliveBatchSize();
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