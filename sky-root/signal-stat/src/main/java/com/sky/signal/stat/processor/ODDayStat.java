package com.sky.signal.stat.processor;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.*;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/30 15:57
 * description: 基站日OD统计分析
 */
@Service("odDayStat")
public class ODDayStat implements Serializable{
    @Autowired
    private transient ParamProperties params;

    public DataFrame process(DataFrame ODDf) {
        DataFrame df = ODDf.groupBy("date", "leave_geo", "arrive_geo", "person_class", "trip_purpose","sex", "age_class")
                .agg(count("*").as("trip_num"), countDistinct("msisdn").as("num_inter"), sum("move_time").as("sum_time"), sum("distance").as("sum_dis"))
                .withColumn("avg_time",floor(col("sum_time").divide(col("trip_num")).divide(60)))
                .withColumn("avg_dis", floor(col("sum_dis").divide(col("trip_num")))).drop("sum_time").drop("sum_dis")
                .orderBy("date","leave_geo", "arrive_geo", "person_class", "trip_purpose","sex", "age_class");
        FileUtil.saveFile(df.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getOdDaySavePath());
        return df;

    }
}
