package com.sky.signal.pre.service.impl.TransportationHub;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.transportationhub
        .StationPersonClassification;
import com.sky.signal.pre.service.ComputeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by Chenhu on 2020/5/27.
 * 枢纽站人口分类服务
 */
@Component
public class StationPersonClassificationService implements ComputeService {
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient StationPersonClassification stationPersonClassification;
    @Override
    public void compute() {
        for (String stationTraceFile: params.getStationTraceFileFullPath()) {
            stationPersonClassification.process(stationTraceFile);
        }
    }
}
