package com.sky.signal.stat.processor;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.util.FileUtil;
import com.sky.signal.stat.util.ProfileUtil;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.countDistinct;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/30 15:57
 * description: 日出行总体特征
 */
@Service("dayTripSummaryStat")
public class DayTripSummaryStat implements Serializable{
    @Autowired
    private transient ParamProperties params;

    public DataFrame process(DataFrame ODDf) {
        DataFrame df = ODDf.groupBy("date", "person_class", "sex", "age_class")
                .agg(count("*").as("trip_num"), countDistinct("msisdn").as("num_inter"))
                .orderBy("date","person_class", "sex", "age_class");
        FileUtil.saveFile(df.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getStatPathWithProfile() + "day-trip-summary-stat");
        return df;

    }
}
