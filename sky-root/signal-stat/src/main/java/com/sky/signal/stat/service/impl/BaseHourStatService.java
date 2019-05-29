package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.BaseHourStat;
import com.sky.signal.stat.processor.signal.SignalLoader;
import com.sky.signal.stat.processor.workLive.WorkLiveLoader;
import com.sky.signal.stat.service.ComputeService;
import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("baseHourStatService")
public class BaseHourStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(BaseHourStatService.class);

    @Autowired
    private transient SignalLoader signalLoader;
    @Autowired
    private transient WorkLiveLoader workLiveLoader;
    @Autowired
    private transient BaseHourStat baseHourStat;
    @Autowired
    private transient ParamProperties params;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DataFrame validSignalDf = signalLoader.load(params.getValidSignalFilesForStat());
        DataFrame workLiveDf = workLiveLoader.load(params.getWorkLiveFile());
        // 基站每1小时人口统计特征
        baseHourStat.process(validSignalDf, workLiveDf);
        logger.info("BaseHourStatService duration: " + stopwatch.toString());
    }
}
