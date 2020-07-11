package com.sky.signal.population.processor;

import org.apache.spark.sql.DataFrame;
import org.springframework.stereotype.Service;

import java.io.Serializable;

/**
 * Created by Chenhu on 2020/7/11.
 * 目标区县信令加载
 * 目标信令有可能位于一个以区县编码命名的目录下，
 * 也有可能位于省级信令目录下
 * 如果位于省级目录下，需要遍历所有信令找到目标信令
 * 如果位于区县目录下，可以直接加载
 */
@Service
public class DestDistrictSignalLoader implements Serializable {

    public DataFrame loader() {
        return null;
    }
}
