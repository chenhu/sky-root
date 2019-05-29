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
        DataFrame agg3 = dfWithCRM.groupBy("date").agg(sum(col("date_count1")).as("date_count1"), sum(col("date_count2")).as("date_count2"));
        // 统计没有匹配到基站的数据
        DataFrame agg4 = tuple2._2().groupBy(col("date")).agg(count("msisdn").as("no_base_count"));

        String date = params.getStrYear() + params.getStrMonth() + params.getStrDay();
        // 保存结果
        FileUtil.saveFile(agg1.repartition(1), FileUtil.FileType.CSV, params.getSavePath() + "stat/dataquality/" + date + "/stat1");
        FileUtil.saveFile(agg2.repartition(1), FileUtil.FileType.CSV, params.getSavePath() + "stat/dataquality/" + date + "/stat2");
        FileUtil.saveFile(agg3.repartition(1), FileUtil.FileType.CSV, params.getSavePath() + "stat/dataquality/" + date + "/stat3");
        FileUtil.saveFile(agg4.repartition(1), FileUtil.FileType.CSV, params.getSavePath() + "stat/dataquality/" + date + "/stat4");

        validDf.unpersist();
    }
}
