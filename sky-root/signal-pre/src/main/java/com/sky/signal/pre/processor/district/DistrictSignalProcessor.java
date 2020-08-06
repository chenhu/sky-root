package com.sky.signal.pre.processor.district;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.signalProcess.SignalSchemaProvider;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
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
public class DistrictSignalProcessor implements Serializable {

    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient JavaSparkContext sparkContext;

    @Autowired
    private transient DistrictMsisdnProcessor districtMsisdnProcessor;

    public void process() {
        //加载全省信令，按天处理
        for (String date : params.getStrDay().split(",")) {
            final Broadcast<Map<String, Boolean>> msisdnVar = districtMsisdnProcessor.load(params.getDistrictCode(),
                    params.getCityCode().toString(),
                    date);
            for (String cityCode : params.JS_CITY_CODES) {
                OneDaySignalProcess(date, cityCode, msisdnVar);
            }
        }
    }

    private void OneDaySignalProcess(String date, String cityCode, final Broadcast<Map<String, Boolean>> msisdnVar) {
        String tracePath = params.getTraceFiles(cityCode, date);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
        DataFrame sourceDf = sqlContext.read().parquet(tracePath).repartition(params.getPartitions());
        JavaRDD<Row> resultOdRdd = sourceDf.javaRDD().filter(new Function<Row, Boolean>() {
            final Map<String, Boolean> msisdnMap = msisdnVar.getValue();
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
        FileUtil.saveFile(resultDf, FileUtil.FileType.PARQUET, params.getDistrictTraceSavePath(params.getDistrictCode(), cityCode, date));
    }

    /**
     * <pre>
     * 从路径中获取当前处理的信令日期
     * 因为路径格式经常变化的，所以这个方法会经常跟着路径改动而变动
     * 20200803-当前处理数据的路径格式为
     * /user/bdoc/17/services/hdfs/132/batch1/jiangsu/trace/dt=yyyyMMdd/citycode=xxxxxx/
     * </pre>
     *
     * @param path 当前处理的信令路径
     * @return 当前处理的信令日期
     */
    private String getDateFromPath(String path) {
        String[] paths = path.split("/", -1);
        return paths[10].substring(3);
    }

}
