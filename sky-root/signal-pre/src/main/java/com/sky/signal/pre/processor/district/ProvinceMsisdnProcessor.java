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
 * @Description
 * 抽取全省信令中，一天内出现在两个或者两个以上区县的手机号码
 * @Author chenhu
 * @Date 2020/8/4 09:25
 **/
@Component
public class ProvinceMsisdnProcessor implements Serializable {
    @Autowired
    private transient CellLoader cellLoader;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SignalLoader signalLoader;
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient JavaSparkContext sparkContext;

    public void process() {
        final Broadcast<Map<String, Row>> cellVar = cellLoader.load(params.getCellSavePath());
        for (String date : params.getStrDay().split(",")) {
            //加载要处理的地市的信令
            String tracePath = params.getTraceFiles(Integer.valueOf(date));
            //合并基站信息到信令数据中
            DataFrame sourceDf = FileUtil.readFile(FileUtil.FileType.PARQUET,null, tracePath).repartition(params.getPartitions());
            sourceDf = signalLoader.cell(cellVar).mergeCell(sourceDf)
                    .groupBy("msisdn","district_code")
                    .agg(count("msisdn").as("num")).filter(col("num").geq(2))
                    .select("msisdn").dropDuplicates();
            FileUtil.saveFile(sourceDf, FileUtil.FileType.PARQUET, params.getDistrictMsisdnSavePath(date));
        }
    }

    public Broadcast<Map<String, Boolean>> load(String date) {
        DataFrame msisdnDf =
                FileUtil.readFile(FileUtil.FileType.PARQUET,MsisdnSchemaProvider.MSISDN,params.getDistrictMsisdnSavePath(date)).repartition(params.getPartitions());
        List<Row> msisdnRowList = msisdnDf.collectAsList();
        Map<String, Boolean> msisdnMap = new HashMap<>(msisdnRowList.size());
        for (Row row : msisdnRowList) {
            msisdnMap.put(row.getAs("msisdn").toString(),true );
        }
        return sparkContext.broadcast(msisdnMap);
    }


}
