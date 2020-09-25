package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.WorkLiveStat;
import com.sky.signal.stat.processor.workLive.WorkLiveLoader;
import com.sky.signal.stat.service.ComputeService;
import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("workLiveStatService")
public class WorkLiveStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(WorkLiveStatService.class);
    @Autowired
    private transient WorkLiveLoader workLiveLoader;
    @Autowired
    private transient WorkLiveStat workLiveStat;

    @Autowired
    private transient ParamProperties params;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DataFrame workLiveDf = workLiveLoader.loadMergedWorkLive();
        // 人口居住地及就业地统计表
        workLiveStat.process(workLiveDf);

        logger.info("WorkLiveStatService duration: " + stopwatch.toString());
    }
}
