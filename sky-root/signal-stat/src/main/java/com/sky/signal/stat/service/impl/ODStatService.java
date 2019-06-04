package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.*;
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

@Service("oDStatService")
public class ODStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(ODStatService.class);
    @Autowired
    private transient ODLoader odLoader;
    @Autowired
    private transient CombineODWorkLive combineODWorkLive;
    @Autowired
    private transient WorkLiveLoader workLiveLoader;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient ODDayStat odDayStat;
    @Autowired
    private transient DayTripSummaryStat dayTripSummaryStat;
    @Autowired
    private transient DayTripPurposeSummaryStat dayTripPurposeSummaryStat;
    @Autowired
    private transient ODTimeIntervalStat oDTimeIntervalStat;
    @Autowired
    private transient ODTimeDistanceStat oDTimeDistanceStat;
    @Autowired
    private transient ODTraceStat odTraceStat;
    @Autowired
    private transient SQLContext sqlContext;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DataFrame odDf = odLoader.loadOD();
        DataFrame odTraceDf = odLoader.loadODTrace();
        DataFrame workLiveDf = workLiveLoader.load(params.getWorkLiveFile());
        DataFrame odWorkLiveCombinedDf = combineODWorkLive.process(odDf, workLiveDf);
        odWorkLiveCombinedDf.persist(StorageLevel.DISK_ONLY());
        // 基站日OD表
        odDayStat.process(odWorkLiveCombinedDf);
        // 日出行总体特征
        dayTripSummaryStat.process(odWorkLiveCombinedDf);
        // 分目的的总体特征
        dayTripPurposeSummaryStat.process(odWorkLiveCombinedDf);
        //基站特定时间间隔OD统计
        oDTimeIntervalStat.process(odWorkLiveCombinedDf, sqlContext);
        // 出行时耗-距离分布
        oDTimeDistanceStat.process(odWorkLiveCombinedDf, sqlContext);
        odTraceStat.process(odTraceDf, workLiveDf, sqlContext);
        odWorkLiveCombinedDf.unpersist();
        logger.info("ODStatService duration: " + stopwatch.toString());
    }
}
