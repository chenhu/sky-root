package com.sky.signal.stat.processor;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.util.FileUtil;
import com.sky.signal.stat.util.ProfileUtil;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.countDistinct;

/**
 * description: 人口居住地及就业地统计表
 * param:
 * return:
 **/
@Service("workLiveStat")
public class WorkLiveStat implements Serializable {
    @Autowired
    private transient ParamProperties params;

    public DataFrame process(DataFrame workLiveDF) {

        DataFrame joinedDf = workLiveDF.groupBy("exists_days", "stay_time_class", "live_base", "on_lsd", "uld", "work_base", "on_wsd",
                "uwd", "sex", "age_class", "region", "cen_region").agg(countDistinct("msisdn")
                .as("peo_num")).orderBy(col("exists_days"), col("stay_time_class"), col("live_base"), col("on_lsd"), col("uld"),
                col("work_base"), col("on_wsd"), col("uwd"), col("sex"), col("age_class"), col("region"), col("cen_region")).repartition(1);
        FileUtil.saveFile(joinedDf.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/work-live-stat");
        return joinedDf;
    }
}
