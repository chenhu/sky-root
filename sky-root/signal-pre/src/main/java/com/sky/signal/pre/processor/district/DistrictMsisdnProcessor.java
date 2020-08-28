package com.sky.signal.pre.processor.district;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.baseAnalyze.CellLoader;
import com.sky.signal.pre.processor.signalProcess.SignalLoader;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;

/**
 * @Description 抽取指定区县指定日期出现的手机号码，并保存到特定位置
 * @Author chenhu
 * @Date 2020/8/4 09:25
 **/
@Component
public class DistrictMsisdnProcessor implements Serializable {
    @Autowired
    private transient CellLoader cellLoader;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient JavaSparkContext sparkContext;
    @Autowired
    private transient SignalLoader signalLoader;

    public void process() {
        final Broadcast<Map<String, Row>> cellVar = cellLoader.load(params.getCellSavePath());
        for (String date : params.getStrDay().split(",")) {
            //抽取当前日期要处理的地市下的区县信令中的手机号码
            String tracePath = params.getTraceFiles(params.getCityCode().toString(), date);
            SQLContext sqlContext = new SQLContext(sparkContext);
            DataFrame sourceDf = sqlContext.read().parquet(tracePath).repartition(params.getPartitions());
            sourceDf = signalLoader.cell(cellVar).mergeCell(sourceDf);
            DataFrame msisdnDf = sourceDf.filter(col("district_code").equalTo(params.getDistrictCode())).select("msisdn").dropDuplicates().cache();
            //加载当前处理日期所有地市的信令
            String traceOfDay = params.getTraceFiles(Integer.valueOf(date));
            DataFrame traceOfDayDf = sqlContext.read().parquet(traceOfDay).repartition(params.getPartitions());;
            DataFrame mergedDf = signalLoader.cell(cellVar).mergeCell(traceOfDayDf);
            DataFrame resultDf = msisdnDf.join(mergedDf,msisdnDf.col("msisdn").equalTo(mergedDf.col("msisdn")),"left_outer")
                    .drop(msisdnDf.col("msisdn")).filter(col("msisdn").isNotNull());
            resultDf = resultDf.groupBy("msisdn","district_code")
                    .agg(count("msisdn").as("num")).filter(col("num").geq(2))
                    .select("msisdn").dropDuplicates();
            FileUtil.saveFile(resultDf, FileUtil.FileType.PARQUET, params.getDistrictMsisdnSavePath(params.getDistrictCode(), params.getCityCode().toString(), date));
        }
    }

    public Broadcast<Map<String, Boolean>> load(Integer districtCode, String cityCode, String date) {
        DataFrame msisdnDf = FileUtil.readFile(FileUtil.FileType.PARQUET,MsisdnSchemaProvider.MSISDN,params.getDistrictMsisdnSavePath(districtCode,cityCode,date));
        List<Row> msisdnRowList = msisdnDf.collectAsList();
        Map<String, Boolean> msisdnMap = new HashMap<>(msisdnRowList.size());
        for (Row row : msisdnRowList) {
            msisdnMap.put(row.getAs("msisdn").toString(),true );
        }
        return sparkContext.broadcast(msisdnMap);
    }
}
