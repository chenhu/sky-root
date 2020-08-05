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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

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
            //加载要处理的地市的信令
            String tracePath = params.getTraceFiles(params.getCityCode().toString(), date);
            SQLContext sqlContext = new SQLContext(sparkContext);
            //合并基站信息到信令数据中
            DataFrame sourceDf = sqlContext.read().parquet(tracePath).repartition(params.getPartitions());
            sourceDf = signalLoader.cell(cellVar).mergeCell(sourceDf);
            DataFrame msisdnDf = sourceDf.filter(col("district_code").equalTo(params.getDistrictCode())).select("msisdn").dropDuplicates();
            FileUtil.saveFile(msisdnDf, FileUtil.FileType.PARQUET, params.getDistrictMsisdnSavePath(params.getDistrictCode(), params.getCityCode().toString(), date));
        }
    }

    public Broadcast<List<String>> load(Integer districtCode, String cityCode, String date) {
        DataFrame msisdnDf = FileUtil.readFile(FileUtil.FileType.PARQUET,MsisdnSchemaProvider.MSISDN,params.getDistrictMsisdnSavePath(districtCode,cityCode,date));
        List<Row> msisdnRowList = msisdnDf.collectAsList();
        List<String> msisdnList = new ArrayList<>(msisdnRowList.size());
        for (Row row : msisdnRowList) {
            msisdnList.add(row.getAs("msisdn").toString());
        }
        return sparkContext.broadcast(msisdnList);
    }


}
