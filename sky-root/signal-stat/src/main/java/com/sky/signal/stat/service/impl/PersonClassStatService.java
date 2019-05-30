package com.sky.signal.stat.service.impl;

import com.google.common.base.Stopwatch;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.PersonClassStat;
import com.sky.signal.stat.processor.workLive.WorkLiveLoader;
import com.sky.signal.stat.service.ComputeService;
import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service("personClassStatService")
public class PersonClassStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(PersonClassStatService.class);
    @Autowired
    private transient WorkLiveLoader workLiveLoader;
    @Autowired
    private transient PersonClassStat personClassStat;

    @Autowired
    private transient ParamProperties params;

    @Override
    public void compute() {
        Stopwatch stopwatch = Stopwatch.createStarted();
        DataFrame workLiveDf = workLiveLoader.load1(params.getWorkLiveFile());
        // 按照人口分类进行统计数量
        personClassStat.process(workLiveDf);

        logger.info("personClassStatService duration: " + stopwatch.toString());
    }
}
