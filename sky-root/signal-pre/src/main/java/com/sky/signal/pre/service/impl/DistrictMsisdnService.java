package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.processor.district.DistrictMsisdnProcessor;
import com.sky.signal.pre.service.ComputeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Description 抽取指定日期指定区县出现的msisdn，并持久化
 * @Author chenhu
 * @Date 2020/8/4 09:23
 **/
@Service
public class DistrictMsisdnService implements ComputeService {
    @Autowired
    private DistrictMsisdnProcessor districtMsisdnProcessor;
    @Override
    public void compute() {
        districtMsisdnProcessor.process();
    }
}
