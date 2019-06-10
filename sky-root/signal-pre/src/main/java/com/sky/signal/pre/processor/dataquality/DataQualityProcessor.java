package com.sky.signal.pre.processor.dataquality;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.baseAnalyze.CellLoader;
import com.sky.signal.pre.processor.crmAnalyze.CRMProcess;
import com.sky.signal.pre.processor.signalProcess.SignalLoader;
import com.sky.signal.pre.processor.signalProcess.SignalSchemaProvider;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Map;

import static org.apache.spark.sql.functions.*;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/5/13 17:24
 * description:
 */
@Service
public class DataQualityProcessor {
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient JavaSparkContext sparkContext;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient CellLoader cellLoader;
    @Autowired
    private transient CRMProcess crmProcess;
    @Autowired
    private transient SignalLoader signalLoader;

    public void process(String traceFilePath) {
        JavaRDD<String> lines = sparkContext.textFile(traceFilePath).repartition(params.getPartitions());
        //基站信息
        final Broadcast<Map<String, Row>> cellVar = cellLoader.load();

        //CRM信息
        final Broadcast<Map<String, Row>> userVar = crmProcess.process();

        //补全基站信息并删除重复信令
        Tuple2<DataFrame, DataFrame> tuple2 =signalLoader.cell(cellVar).mergeCell(lines);
        // 统计正常数据分布情况
        DataFrame validDf = tuple2._1().persist(StorageLevel.DISK_ONLY());
        DataFrame agg1 = validDf.groupBy("date").agg(countDistinct("msisdn").as("msisdn_count"),
                count("msisdn").as("valid_row_count"),
                countDistinct("base").as("base_count"));
        JavaRDD<Row> validRDD = signalLoader.crm(userVar).mergeCRM1(validDf.javaRDD());
        DataFrame dfWithCRM = sqlContext.createDataFrame(validRDD, SignalSchemaProvider.SIGNAL_SCHEMA_BASE_3);
        // 没有找到crm信息的手机信令
        DataFrame agg2 = dfWithCRM.filter(col("cen_region").equalTo(0)).groupBy("date").agg(count("msisdn").as("no_crm_count"));
        // 统计date和开始日期或者结束日期不同天的数据,以及开始日期和结束日期不同天的数据
        DataFrame agg3 = dfWithCRM.groupBy("date").agg(sum(col("date_count1")).as("dt_error_count1"), sum(col("date_count2")).as("dt_error_count2"));
        DataFrame allSignalDf = tuple2._2().persist(StorageLevel.DISK_ONLY());
        // 统计没有匹配到基站的数据
        DataFrame inValidDf = allSignalDf.filter(col("base").equalTo("null").and(col("lng").equalTo(0)).and(col("lat")
                .equalTo(0)));
        DataFrame agg4 = inValidDf.groupBy(col("date")).agg(count("msisdn").as("no_base_count"));

        DataFrame agg5 = allSignalDf.groupBy("date").agg(count("*").as("orginal_count"));
        DataFrame joinedDf = agg1.join(agg2, agg1.col("date").equalTo(agg2.col("date")))
                .join(agg3, agg1.col("date").equalTo(agg3.col("date")))
                .join(agg4, agg1.col("date").equalTo(agg4.col("date")))
                .join(agg5, agg1.col("date").equalTo(agg5.col("date")))
                .select(
                        agg1.col("date"),
                        agg5.col("orginal_count"),
                        agg1.col("valid_row_count"),
                        agg1.col("base_count"),
                        agg2.col("no_crm_count"),
                        agg3.col("dt_error_count1"),
                        agg3.col("dt_error_count2"),
                        agg4.col("no_base_count"),
                        agg1.col("msisdn_count")
                );

        String date = params.getStrYear() + params.getStrMonth() + params.getStrDay();
        // 保存结果
        FileUtil.saveFile(joinedDf.repartition(1), FileUtil.FileType.CSV, params.getSavePath() + "stat/dataquality/" + date + "/stat");

        validDf.unpersist();
        allSignalDf.unpersist();
    }
}
