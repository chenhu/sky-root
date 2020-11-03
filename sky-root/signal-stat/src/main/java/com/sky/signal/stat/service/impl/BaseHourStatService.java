package com.sky.signal.stat.service.impl;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.BaseHourStat;
import com.sky.signal.stat.processor.signal.SignalSchemaProvider;
import com.sky.signal.stat.processor.workLive.WorkLiveLoader;
import com.sky.signal.stat.service.ComputeService;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service("baseHourStatService")
public class BaseHourStatService implements ComputeService {
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient WorkLiveLoader workLiveLoader;
    @Autowired
    private transient BaseHourStat baseHourStat;

    @Override
    public void compute() {
        this.clearBaseHourTempData();
        DataFrame workLiveDf = workLiveLoader.loadMergedWorkLive().select("msisdn","person_class","sex","age_class").repartition(params.getPartitions());
        workLiveDf = workLiveDf.persist(StorageLevel.DISK_ONLY());
        Map<Integer, List<String>> validSignalFileMap = FilesBatchUtils.getBatchFiles(params.getValidSignalFilesForStat(), params.getStatBatchSize(),params.getCrashPosition());
        for( int batchId: validSignalFileMap.keySet()) {
            List<String> validSignalFiles = validSignalFileMap.get(batchId);
            baseHourStat.process(getValidSignal(validSignalFiles),workLiveDf, batchId);
        }
        baseHourStat.agg();
        workLiveDf.unpersist();
    }
    private DataFrame getValidSignal(List<String> validSignalFiles) {
        DataFrame validSignalDF = null;
        for (String ValidSignalFile : validSignalFiles) {
            DataFrame validDF = FileUtil.readFile(FileUtil.FileType.PARQUET, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA, ValidSignalFile)
                    .select("msisdn","date","base","lat","lng","begin_time","last_time");
            if (validSignalDF == null) {
                validSignalDF = validDF;
            } else {
                validSignalDF = validSignalDF.unionAll(validDF);
            }
        }

        return validSignalDF.repartition(params.getPartitions());
    }

    private void clearBaseHourTempData() {
        FileUtil.removeDfsDirectory(params.getBaseHourTempSavePath());

    }
}
