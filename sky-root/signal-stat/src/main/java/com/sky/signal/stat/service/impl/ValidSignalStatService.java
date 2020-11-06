package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.ValidSignalStat;
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

import java.util.List;
import java.util.Map;

@Service("validSignalStatService")
public class ValidSignalStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(ValidSignalStatService.class);
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient WorkLiveLoader workLiveLoader;
    @Autowired
    private transient ValidSignalStat validSignalStat;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        this.clearValidStatTempData();
        DataFrame workLiveDf = workLiveLoader.loadMergedWorkLive().select("msisdn","person_class").repartition(params.getPartitions()).persist(StorageLevel.DISK_ONLY());
        Map<Integer, List<String>> validSignalFileMap = FilesBatchUtils.getBatchFiles(params.getValidSignalFilesForStat(), params.getStatBatchSize(),params.getCrashPosition());
        for( int batchId: validSignalFileMap.keySet()) {
            List<String> validSignalFiles = validSignalFileMap.get(batchId);
            validSignalStat.process(getValidSignal(validSignalFiles),workLiveDf, batchId);
        }
        validSignalStat.agg();
        workLiveDf.unpersist();
        logger.info("ValidSignalStatService duration: " + stopwatch.toString());
    }
    private DataFrame getValidSignal(List<String> validSignalFiles) {
        DataFrame validSignalDF = null;
        for (String ValidSignalFile : validSignalFiles) {
            DataFrame validDF = FileUtil.readFile(FileUtil.FileType.PARQUET, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA, ValidSignalFile).select("date","msisdn");
            if (validSignalDF == null) {
                validSignalDF = validDF;
            } else {
                validSignalDF = validSignalDF.unionAll(validDF);
            }
        }

        return validSignalDF.repartition(params.getPartitions());
    }

    private void clearValidStatTempData() {
        FileUtil.removeDfsDirectory(params.getValidSignalStatTempSavePath());
    }
}
