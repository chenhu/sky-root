package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.processor.dataSelector.SelectDate;
import com.sky.signal.pre.service.ComputeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

// 暂时不需要，也不知道这个用处是什么 20190320 ChenHu
@Component("SelectDateService")
public class SelectDateService implements ComputeService {

    @Autowired
    private SelectDate selectDate;
    @Override
    public void compute() {
        selectDate.process();
    }
}
