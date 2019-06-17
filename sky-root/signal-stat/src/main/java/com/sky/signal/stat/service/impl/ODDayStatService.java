package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.processor.CombineODWorkLive;
import com.sky.signal.stat.processor.ODDayStat;
import com.sky.signal.stat.service.ComputeService;
import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("oDDayStatService")
public class ODDayStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(ODDayStatService.class);
    @Autowired
    private transient CombineODWorkLive combineODWorkLive;
    @Autowired
    private transient ODDayStat odDayStat;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DataFrame odWorkLiveCombinedDf = combineODWorkLive.read();
        // 基站日OD表
        odDayStat.process(odWorkLiveCombinedDf);
        logger.info("oDDayStatService duration: " + stopwatch.toString());
    }
}
