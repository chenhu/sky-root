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
            final Broadcast<Map<String, Boolean>> msisdnVar = provinceMsisdnProcessor.load(date);
            for(String cityCode: params.JS_CITY_CODES) {
                OneDaySignalProcess(cityCode,date, msisdnVar);
            }

        }
    }

    private void OneDaySignalProcess(String cityCode, String date, final Broadcast<Map<String, Boolean>> msisdnVar) {
        String tracePath = params.getTraceFiles(cityCode,date);
        DataFrame sourceDf = sqlContext.read().format("parquet").load(tracePath).repartition(params.getPartitions());
        JavaRDD<Row> resultOdRdd = sourceDf.javaRDD().filter(new Function<Row, Boolean>() {
            Map<String, Boolean> msisdnMap = msisdnVar.value();
            @Override
            public Boolean call(Row row) throws Exception {
                Boolean value = msisdnMap.get(row.getAs("msisdn").toString());
                if (value != null) {
                    return value;
                } else {
                    return false;
                }
            }
        });
        DataFrame resultDf = sqlContext.createDataFrame(resultOdRdd, SignalSchemaProvider.SIGNAL_SCHEMA_ORIGN).repartition(params.getPartitions());
        FileUtil.saveFile(resultDf, FileUtil.FileType.PARQUET, params.getTraceSavePath(cityCode,date));
    }
}
