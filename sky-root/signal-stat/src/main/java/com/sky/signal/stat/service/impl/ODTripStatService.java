package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.processor.CombineODWorkLive;
import com.sky.signal.stat.processor.DayTripPurposeSummaryStat;
import com.sky.signal.stat.processor.DayTripSummaryStat;
import com.sky.signal.stat.service.ComputeService;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("oDTripStatService")
public class ODTripStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(ODTripStatService.class);
    @Autowired
    private transient CombineODWorkLive combineODWorkLive;
//    @Autowired
//    private transient CombineTripStatWorkLive combineTripStatWorkLive;
//    @Autowired
//    private transient ODTripStat odTripStat;
    @Autowired
    private transient DayTripSummaryStat dayTripSummaryStat;
    @Autowired
    private transient DayTripPurposeSummaryStat dayTripPurposeSummaryStat;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DataFrame odWorkLiveCombinedDf = combineODWorkLive.read();
//        DataFrame odTripStatCombinedDf = combineTripStatWorkLive.read();
        odWorkLiveCombinedDf.persist(StorageLevel.DISK_ONLY());
        // 日出行总体特征
        dayTripSummaryStat.process(odWorkLiveCombinedDf);
        //临时统计
//        dayTripSummaryStat.personTripNumStat(odWorkLiveCombinedDf);
        // 分目的的总体特征
        dayTripPurposeSummaryStat.process(odWorkLiveCombinedDf);
//        odTripStat.process(odTripStatCombinedDf);
        odWorkLiveCombinedDf.unpersist();
        logger.info("oDTripStatService duration: " + stopwatch.toString());
    }
}
