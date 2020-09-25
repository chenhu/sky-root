package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.MigrateStat;
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

/**
 * 对外人口流动情况统计表
 */
@Service("migrateStatService")
public class MigrateStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(MigrateStatService.class);
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient WorkLiveLoader workLiveLoader;
    @Autowired
    private transient MigrateStat migrateStat;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DataFrame workLiveDf = workLiveLoader.loadMergedWorkLive().select("msisdn", "region").repartition(params.getPartitions()).persist(StorageLevel.DISK_ONLY());
        Map<Integer, List<String>> validSignalFileMap = FilesBatchUtils.getBatchFiles(params.getValidSignalFilesForStat(), params.getStatBatchSize(),params.getCrashPosition());
        for (int batchId : validSignalFileMap.keySet()) {
            List<String> validSignalFiles = validSignalFileMap.get(batchId);
            migrateStat.process(getValidSignal(validSignalFiles), workLiveDf, batchId);
        }
        migrateStat.agg();
        workLiveDf.unpersist();
        logger.info("BaseHourStatService duration: " + stopwatch.toString());
    }

    private DataFrame getValidSignal(List<String> validSignalFiles) {
        DataFrame validSignalDF = null;
        for (String ValidSignalFile : validSignalFiles) {
            DataFrame validDF = FileUtil.readFile(FileUtil.FileType.PARQUET, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA, ValidSignalFile).select("msisdn", "date", "base", "lat", "lng", "begin_time", "last_time");
            if (validSignalDF == null) {
                validSignalDF = validDF;
            } else {
                validSignalDF = validSignalDF.unionAll(validDF);
            }
        }

        return validSignalDF.repartition(params.getPartitions());
    }
}
