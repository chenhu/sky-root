package com.sky.signal.pre.processor.transportationhub;

import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;

/**
 * Created by Chenhu on 2020/5/25.
 * 枢纽站轨迹预处理流程
 */
@Component
public class TransportationHubProcessor implements Serializable {
    @Autowired
    private transient StationDataFilter stationDataFilter;

    public void process(DataFrame validSignalDF) {
        //1.从所有数据中提取出占用枢纽基站的用户。若该用户当天的所有轨迹中出现枢纽基站，则保留该用户当天全部的轨迹信息。
        // 并合并相邻枢纽基站
        DataFrame stationBaseValidSignalDf = stationDataFilter.filterData
                (validSignalDF);
        //2. 人口分类
        //2.2 判定停留点类型
        //2.3 根据用户当天逗留时间，进行枢纽站人口分类判定或者铁路过境人口判断
        //2.3.1 铁路过境人口判断
        //2.3.2 枢纽站人口分类判定
    }

}
