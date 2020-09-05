package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.odAnalyze.StayPointProcessor;
import com.sky.signal.pre.service.ComputeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/3/20 14:37
 * description: 根据有效数据，按天生成用户OD信息
 */
@Service("odService")
public class ODService implements ComputeService {
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient StayPointProcessor stayPointProcessor;

    @Override
    public void compute() {
        for (String validSignalFile : params.getValidSignalListByDays()) {
            stayPointProcessor.process(validSignalFile);
        }

    }
}
