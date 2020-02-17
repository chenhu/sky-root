package com.sky.signal.stat.processor;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.od.ODSchemaProvider;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/6/16 17:07
 * description: 合并出行统计数据和职住数据，用来按照人口分类统计每天多少人有出行，多人无出行（只有一个逗留点、多个逗留点、无逗留点）
 */
@Service("combineTripStatWorkLive")
public class CombineTripStatWorkLive implements Serializable {
    @Autowired
    private transient ParamProperties params;
    public DataFrame process(DataFrame statTripDf, DataFrame workLiveStat, Integer batchId) {
        DataFrame odTripStat = statTripDf.join(workLiveStat, statTripDf.col("msisdn").equalTo(workLiveStat.col("msisdn")), "left_outer");
        odTripStat = odTripStat.select(
                statTripDf.col("date"),
                statTripDf.col("msisdn"),
                workLiveStat.col("person_class"),
                statTripDf.col("has_trip"),
                statTripDf.col("staypoint_count")
                );
        FileUtil.saveFile(odTripStat, FileUtil.FileType.CSV, params.getSavePath() + "stat/od_trip_stat/"+batchId+"/combine-statTrip");

        return odTripStat;
    }

    public DataFrame read() {
        return FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider.OD_TRIP_STAT_SCHEMA,params.getSavePath() + "stat/od_trip_stat/*/combine-statTrip").repartition(params.getPartitions());
    }
}
