package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.district.DistrictMsisdnProcessor;
import com.sky.signal.pre.processor.district.ProvinceMsisdnProcessor;
import com.sky.signal.pre.service.ComputeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description 抽取指定日期指定区县出现的msisdn，并持久化
 * @Author chenhu
 * @Date 2020/8/4 09:23
 **/
@Service
public class MsisdnCollectionService implements ComputeService {
    @Autowired
    private DistrictMsisdnProcessor districtMsisdnProcessor;
    @Autowired
    private ProvinceMsisdnProcessor provinceMsisdnProcessor;
    @Autowired
    private ParamProperties params;
    @Override
    public void compute() {
        if(params.getRunMode().equals("province")) {
            provinceMsisdnProcessor.process();
        } else if(params.getRunMode().equals("district")) {
            districtMsisdnProcessor.process();
        }

    }
}
