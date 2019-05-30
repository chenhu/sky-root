package com.sky.signal.stat.processor;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.count;

/**
 * description: 按照人口分类统计人口数量
 * param:
 * return:
 **/
@Service("personClassStat")
public class PersonClassStat implements Serializable {
    @Autowired
    private transient ParamProperties params;

    public DataFrame process(DataFrame workLiveDF) {

        DataFrame joinedDf = workLiveDF.groupBy("person_class").agg(count("*").as("peo_num")).repartition(1);
        FileUtil.saveFile(joinedDf.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/person-class-stat");
        return joinedDf;
    }
}
