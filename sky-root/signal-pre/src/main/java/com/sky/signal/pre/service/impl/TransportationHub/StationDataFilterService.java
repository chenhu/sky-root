package com.sky.signal.pre.service.impl.TransportationHub;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.transportationhub.StationDataFilter;
import com.sky.signal.pre.service.ComputeService;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * Created by Chenhu on 2020/5/27.
 * 枢纽站区域信令数据预处理服务
 */
@Component
@Log4j2
public class StationDataFilterService implements ComputeService {
    @Autowired
    private transient ParamProperties params;
    @Autowired private transient StationDataFilter stationDataFilter;
    @Override
    public void compute() {
        for (String validSignalFile: params.getValidSignalListByDays()) {
            stationDataFilter.filterData(validSignalFile);
        }
    }
}
