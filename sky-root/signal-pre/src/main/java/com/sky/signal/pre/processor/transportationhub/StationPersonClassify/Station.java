package com.sky.signal.pre.processor.transportationhub.StationPersonClassify;

import org.apache.spark.sql.DataFrame;

/**
 * Created by Chenhu on 2020/5/27.
 * 枢纽站分类器接口
 */
public interface Station {

    /**
     * 分类接口，实现对枢纽人口人口分类
     * @param df 信令数据
     * @return 带有人口分类信息的信令数据
     */
    DataFrame classify(DataFrame df);

}
