package com.sky.signal.pre.processor.dataquality;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.signalProcess.SignalLoader;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import static org.apache.spark.sql.functions.*;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/5/13 17:24
 * description:
 */
@Service
public class DataQualityProcessor {
    @Autowired
    private transient JavaSparkContext sparkContext;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SignalLoader signalLoader;

    public void process(String date, String traceFilePath, String validSignalFilePath) {
        SQLContext sc = new SQLContext(sparkContext);
        DataFrame orginalSignal =sc.read().parquet(traceFilePath).repartition(params.getPartitions());
        // 统计正常数据分布情况
        DataFrame validSignal = signalLoader.load(validSignalFilePath).repartition(params.getPartitions());
        DataFrame agg1 = validSignal.groupBy("date").agg(countDistinct("msisdn").as("msisdn_count"),
                count("msisdn").as("valid_row_count"),
                countDistinct("base").as("base_count"));
        DataFrame agg2 = orginalSignal.withColumn("date", date_format(col("start_time"),"yyyyMMdd")).groupBy("date").agg(count("*").as("orginal_count"));
        DataFrame joinedDf = agg1.join(agg2, agg1.col("date").equalTo(agg2.col("date")))
                .select(
                        agg1.col("date"),
                        agg2.col("orginal_count"),
                        agg1.col("valid_row_count"),
                        agg1.col("base_count"),
                        agg1.col("msisdn_count")
                );
        // 保存结果
        FileUtil.saveFile(joinedDf.repartition(1), FileUtil.FileType.CSV, params.getDataQualitySavePath(date));
    }
}
