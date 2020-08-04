package com.sky.signal.pre.processor;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.baseAnalyze.CellLoader;
import com.sky.signal.pre.processor.signalProcess.SignalLoader;
import com.sky.signal.pre.processor.signalProcess.SignalSchemaProvider;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.ProfileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * @Description 抽取指定区县的原始信令数据，并保存到特定位置
 * @Author chenhu
 * @Date 2020/8/4 09:25
 **/
@Component
public class DistrictSignalProcessor implements Serializable {

    @Autowired
    private transient CellLoader cellLoader;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient JavaSparkContext sparkContext;

    @Autowired
    private transient SignalLoader signalLoader;

    public void process() {
        //加载全省信令，按天处理
        for (String date : params.getStrDay().split(",")) {
            Broadcast<List<String>> msisdnVar = getExistsMsisdn(date);
            for (String cityCode : params.JS_CITY_CODES) {
                OneDaySignalProcess(date, cityCode,msisdnVar);
            }
        }
    }

    private Broadcast<List<String>> getExistsMsisdn(String date) {
        final Broadcast<Map<String, Row>> cellVar = cellLoader.load(params.getCellSavePath());
        //加载要处理的地市的信令
        String tracePath = params.getTraceFiles(params.getCityCode().toString(), date);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
        //合并基站信息到信令数据中
        DataFrame sourceDf = sqlContext.read().parquet(tracePath).repartition(params.getPartitions());
        sourceDf = signalLoader.cell(cellVar).mergeCell(sourceDf).persist(StorageLevel.DISK_ONLY());
        DataFrame msisdnDf = sourceDf.filter(col("district_code").equalTo(params.getDistrictCode())).select("msisdn").dropDuplicates();
        msisdnDf.show();
        List<Row> msisdnRowList = msisdnDf.collectAsList();
        List<String> msisdnList = new ArrayList<>(msisdnRowList.size());
        for (Row row : msisdnRowList) {
            msisdnList.add(row.getAs("msisdn").toString());
        }
        return sparkContext.broadcast(msisdnList);

    }
    private void OneDaySignalProcess(String date, String cityCode, final Broadcast<List<String>> msisdnVar) {
        String tracePath = params.getTraceFiles(cityCode, date);
        SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkContext);
        //合并基站信息到信令数据中
        DataFrame sourceDf = sqlContext.read().parquet(tracePath).repartition(params.getPartitions());
        JavaRDD<Row> resultOdRdd = sourceDf.javaRDD().filter(new Function<Row, Boolean>() {
            final List<String> msisdnList = msisdnVar.getValue();
            @Override
            public Boolean call(Row row) throws Exception {
                return msisdnList.contains(row.getAs("msisdn"));
            }
        });
        FileUtil.saveFile(sqlContext.createDataFrame(resultOdRdd, SignalSchemaProvider.SIGNAL_SCHEMA_ORIGN).repartition(params.getPartitions()),
                FileUtil.FileType.PARQUET, params.getDistrictTraceSavePath(params.getDistrictCode(), cityCode, date));

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
