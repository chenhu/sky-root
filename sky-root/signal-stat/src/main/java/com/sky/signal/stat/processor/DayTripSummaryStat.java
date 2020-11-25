package com.sky.signal.stat.processor;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.util.FileUtil;
import com.sky.signal.stat.util.ProfileUtil;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

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
        FileUtil.saveFile(df.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getDayTripSummaryStatSavePath());
        return df;

    }

    public DataFrame personTripNumStat(DataFrame ODDf) {
        DataFrame df = ODDf.groupBy("date", "person_class", "msisdn")
                .agg(count("*").as("trip_num")).groupBy("date","person_class","trip_num").agg(countDistinct("msisdn").as("num_inter"))
                .orderBy("date","person_class", "trip_num");
        FileUtil.saveFile(df.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getPersonTripNumSavePath());
        return df;

    }


}
