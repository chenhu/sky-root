package com.sky.signal.population.service;

import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.processor.CellLoader;
import com.sky.signal.population.processor.OdProcess;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

/**
 * Created by Chenhu on 2020/7/11.
 * <pre>
 * 区县OD统计服务
 * 包括：
 * 对外出行OD 表
 * 对外出行OD(含出行人数)
 * 对外出行率统计表
 *
 * </pre>
 */
@Service
@Slf4j
public class DistrictOdStatService implements ComputeService, Serializable {
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient CellLoader cellLoader;
    @Autowired
    private transient OdProcess odProcess;
    @Override
    public void compute() {
    }
}
