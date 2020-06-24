package com.sky.signal.pre.processor.transportationhub.od;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.config.PathConfig;
import com.sky.signal.pre.processor.odAnalyze.ODSchemaProvider;
import com.sky.signal.pre.processor.transportationhub.StationPersonClassifyUtil;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.ProfileUtil;
import com.sky.signal.pre.util.SignalProcessUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;

/**
 * Created by Chenhu on 2020/6/3.
 * 枢纽站人口出行OD分析
 */
@Component
public class ODProcessor implements Serializable {
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;

    @Autowired
    private transient PreODProcess preODProcess;
    @Autowired
    private transient StationPersonClassifyUtil stationPersonClassifyUtil;

    /**
     * 按天进行枢纽站人口OD出行分析
     *
     * @param signalFilePath 带停留点的用户一天信令文件路径
     */
    public void process(String signalFilePath) {
        int partitions = 1;
        if (!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
        final String date = signalFilePath.substring(signalFilePath.length() - 8);

        DataFrame stationTraceDf = FileUtil.readFile(FileUtil.FileType.CSV,
                ODSchemaProvider.TRACE_SCHEMA, signalFilePath)
                .repartition(params.getPartitions());
        // 数据转化为以手机号为key，整行数据为value的KeyPair数据
        JavaPairRDD<String, List<Row>> stationTracePairRDD =
                SignalProcessUtil.signalToJavaPairRDD(stationTraceDf, params);
        // 加载所有枢纽站轨迹有效信令数据，因为OD分析需要用到昨天和明天的信令
        //需要在map外部预先取出这个数据集
        final DataFrame stationTrace = loadStationTrace();
        JavaRDD<List<Row>> stationTraceRDD = stationTracePairRDD.values().map
                (new Function<List<Row>, List<Row>>() {
                    @Override
                    public List<Row> call(List<Row> rows) throws Exception {
                        return getOD(rows, stationTrace);
                    }
                });

        JavaRDD<Row> resultRDD = stationTraceRDD.flatMap(new FlatMapFunction<List<Row>, Row>() {
            @Override
            public Iterable<Row> call(List<Row> rows) throws Exception {
                return rows;
            }
        });
//        DataFrame resultDf = sqlContext.createDataFrame(resultRDD,
//                ODSchemaProvider.STATION_TRACE_CLASSIC_SCHEMA);
//
//        FileUtil.saveFile(resultDf.repartition(partitions), FileUtil.FileType
//                .CSV, params.getSavePath() +
//                PathConfig.STATION_CLASSIC_PATH + date);

    }

    public List<Row> getOD(List<Row> rows, DataFrame df) {
        rows = stationPersonClassifyUtil.mergeStationBase(rows, true, false);
        rows = preODProcess.mergeStayPoint(rows);
        rows = getStationBaseOD(rows, df);
        return rows;
    }

    /**
     * 生成枢纽站人口出行OD
     * <pre>
     * <li>1) 针对铁路出发人口，寻找当天OD轨迹中last_time比枢纽站start_time早且时间间隔最短的停留点作为起始点O
     *     ，枢纽站为终点D；若无法在当天轨迹中找到O点，则取该用户前一天的最后一个停留点作为起始点O；</li>
     * <li>2）	针对铁路到达人口，寻找当天OD轨迹中start_time比枢纽站last_time晚且时间间隔最短的停留点作为终点D
     * ，枢纽站为起始点O；若无法在当天轨迹中找到D点，则取该用户后一天的第一个停留点作为终点D；</li>
     * <li>3）	若1）中寻找到的起始点O以及2）中寻找到的终点D满足下列情况：
     * a.	与枢纽站形成的OD出行距离<= 800m
     * b.	与枢纽站形成的OD出行时耗<= 6min
     * c.	起始点O及终点D都为枢纽站
     * 则向前或向后再寻找下一个停留点为O点/D点，即：针对出发人口，更新起始点O为前一个停留点；针对到达人口，更新终点D为下一个停留点
     * </li>
     * <li>
     * 4）	以O点的基站为出发基站、last_time为出发时间，以D点的基站为到达基站、start_time为到达时间生成枢纽站人口OD链
     * </li>
     * </pre>
     *
     * @param rows 一天的信令
     * @return 枢纽站人口出行OD
     */
    public List<Row> getStationBaseOD(List<Row> rows, DataFrame df) {
        return null;
    }

    /**
     * description: 加载带停留点类型的枢纽站轨迹有效信令数据
     * return: org.apache.spark.sql.DataFrame
     **/
    private DataFrame loadStationTrace() {
        //获取所有的枢纽站轨迹信令
        return FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider
                .TRACE_SCHEMA, params.getSavePath().concat(PathConfig
                .STATION_DATA_PATH).concat("*")).repartition(params.getPartitions()).persist
                (StorageLevel.MEMORY_AND_DISK());

    }
}
