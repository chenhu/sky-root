package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.processor.DistrictSignalProcessor;
import com.sky.signal.pre.service.ComputeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description 抽取指定区县的信令数据，并保存到特定位置
 * @Author chenhu
 * @Date 2020/8/4 09:23
 **/
@Service
public class RestoreDistrictSignalService implements ComputeService {
    @Autowired
    private DistrictSignalProcessor districtSignalProcessor;

    @Override
    public void compute() {
        districtSignalProcessor.process();
    }
}
