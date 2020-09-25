package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.ODTraceStat;
import com.sky.signal.stat.processor.od.ODSchemaProvider;
import com.sky.signal.stat.processor.workLive.WorkLiveLoader;
import com.sky.signal.stat.service.ComputeService;
import com.sky.signal.stat.util.FileUtil;
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
        Map<Integer, List<String>> odTraceMap = FilesBatchUtils.getBatchFiles(params.getODTracePath(), params.getStatBatchSize(),params.getCrashPosition());
        DataFrame workLiveDf = workLiveLoader.loadMergedWorkLive().select("msisdn", "person_class", "sex", "age_class").repartition(params.getPartitions());
        workLiveDf.persist(StorageLevel.DISK_ONLY());
        // 分批次预处理
        for (int batchId : odTraceMap.keySet()) {
            List<String> odTraceFiles = odTraceMap.get(batchId);
            DataFrame odTraceDf = loadODTrace(odTraceFiles);
            odTraceStat.process(odTraceDf, workLiveDf, batchId, sqlContext);
        }
        workLiveDf.unpersist();
        odTraceStat.combineData();

        logger.info("ODTraceStatService duration: " + stopwatch.toString());
    }

    public DataFrame loadODTrace(List<String> odTraceFiles) {
        DataFrame odTraceDf = null;
        for (String odTraceFile : odTraceFiles) {
            if (odTraceDf == null) {
                odTraceDf = FileUtil.readFile(FileUtil.FileType.PARQUET, ODSchemaProvider.OD_TRACE_SCHEMA, odTraceFile).select("date", "msisdn", "leave_base", "arrive_base", "leave_time", "arrive_time");
            } else {
                odTraceDf = odTraceDf.unionAll(FileUtil.readFile(FileUtil.FileType.PARQUET, ODSchemaProvider.OD_TRACE_SCHEMA, odTraceFile).select("date", "msisdn", "leave_base", "arrive_base", "leave_time", "arrive_time"));
            }
        }
        return odTraceDf.repartition(params.getPartitions());
    }
}
