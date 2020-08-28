package com.sky.signal.pre.processor.district;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.signalProcess.SignalSchemaProvider;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import static org.apache.spark.sql.functions.*;

import java.io.Serializable;
import java.util.Map;

/**
 * @Description 抽取指定区县的原始信令数据，并保存到特定位置
 * @Author chenhu
 * @Date 2020/8/4 09:25
 **/
@Component
public class ProvinceSignalProcessor implements Serializable {

    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient ProvinceMsisdnProcessor provinceMsisdnProcessor;


    public void process() {
        //加载全省信令，按天处理
        for (String date : params.getStrDay().split(",", -1)) {
//            final Broadcast<Map<String, Boolean>> msisdnVar = provinceMsisdnProcessor.load(date);
            DataFrame msisdnDf = provinceMsisdnProcessor.loadDf(date).cache();
            for(String cityCode: params.JS_CITY_CODES) {
                OneDaySignalProcess(cityCode,date, msisdnDf);
            }
            msisdnDf.unpersist();

        }
    }
    private void OneDaySignalProcess(String cityCode, String date, DataFrame msisdnDf) {
        String tracePath = params.getTraceFiles(cityCode,date);
        DataFrame sourceDf = sqlContext.read().format("parquet").load(tracePath).repartition(params.getPartitions());
        DataFrame resultDf = msisdnDf.join(sourceDf,msisdnDf.col("msisdn").equalTo(sourceDf.col("msisdn")), "left_outer")
                .drop(msisdnDf.col("msisdn")).filter(col("msisdn").isNotNull());
        FileUtil.saveFile(resultDf, FileUtil.FileType.PARQUET, params.getTraceSavePath(cityCode,date));
    }
}
