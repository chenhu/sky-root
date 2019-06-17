package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.processor.CombineODWorkLive;
import com.sky.signal.stat.processor.ODTimeDistanceStat;
import com.sky.signal.stat.processor.ODTimeIntervalStat;
import com.sky.signal.stat.service.ComputeService;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("oDTimeStatService")
public class ODTimeStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(ODTimeStatService.class);
    @Autowired
    private transient CombineODWorkLive combineODWorkLive;
    @Autowired
    private transient ODTimeIntervalStat oDTimeIntervalStat;
    @Autowired
    private transient ODTimeDistanceStat oDTimeDistanceStat;
    @Autowired
    private transient SQLContext sqlContext;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DataFrame odWorkLiveCombinedDf = combineODWorkLive.read();
        odWorkLiveCombinedDf.persist(StorageLevel.DISK_ONLY());
        //基站特定时间间隔OD统计
        oDTimeIntervalStat.process(odWorkLiveCombinedDf, sqlContext);
        // 出行时耗-距离分布
        oDTimeDistanceStat.process(odWorkLiveCombinedDf, sqlContext);
        odWorkLiveCombinedDf.unpersist();
        logger.info("ODTraceStatService duration: " + stopwatch.toString());
    }
}
