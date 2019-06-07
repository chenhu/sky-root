package com.lesits.ml.service;

import com.lesits.ml.MLService;
import com.lesits.ml.processor.gaode.FeatureDetect;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/5/27 14:43
 * description: 高德路况数据ETL
 */
@Service("etlService")
public class GodeDataETLService implements MLService, Serializable {
    @Autowired
    private transient FeatureDetect featureDetect;
    @Override
    public void process() {
        featureDetect.process();
    }
}
