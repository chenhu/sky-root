package com.sky.signal.population.processor;

import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.util.FileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;

/**
 * @Description 全省部分地市OD方面处理
 * @Author chenhu
 * @Date 2020/8/3 11:44
 **/
@Service
public class OdProcess implements Serializable {
    //在单个区域逗留时间阀值为120分钟
    private static final int DISTRICT_STAY_MINUTE = 2 * 60 * 60;
    @Autowired
    private transient SQLContext sqlContext;
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S");
    private static final DateTimeFormatter FORMATTED = DateTimeFormat.forPattern("yyyyMMdd");
    @Autowired
    private transient ParamProperties params;

    @Autowired
    private transient JavaSparkContext sparkContext;

    /**
     * @Description: 加载基础OD分析结果
     * @Author: Hu Chen
     * @Date: 2020/8/3 11:48
     * @param: []
     * @return: org.apache.spark.sql.DataFrame
     **/
    public DataFrame loadOd() {
        return FileUtil.readFile(FileUtil.FileType.CSV, ODSchemaProvider.OD_SCHEMA, params.getProvinceODFilePath());
    }

    /**
     * @Description: <pre>
     *      在基础OD分析结果中，增加发生地的区县编码
     *  </pre>
     * @Author: Hu Chen
     * @Date: 2020/8/3 14:21
     * @param: provinceCell, od
     * @return: 合并区县代码的OD数据
     **/
    public DataFrame mergeOdWithCell(final Broadcast<Map<String, Row>> provinceCell, DataFrame od) {
        JavaRDD<Row> odRdd = od.toJavaRDD().map(new Function<Row, Row>() {
            Map<String, Row> cellMap = provinceCell.value();

            @Override
            public Row call(Row row) throws Exception {
                String leaveBase = row.getAs("leave_base").toString();
                String arriveBase = row.getAs("arrive_base").toString();
                Integer leaveDistrictCode = 0, arriveDistrictCode = 0;
                try {
                    leaveDistrictCode = Integer.valueOf(cellMap.get(leaveBase).getAs("district_code").toString());
                    arriveDistrictCode = Integer.valueOf(cellMap.get(arriveBase).getAs("district_code").toString());
                } catch (Exception ex) {
                    leaveDistrictCode = 0;
                    arriveDistrictCode = 0;
                }
                return new GenericRowWithSchema(new Object[]{row.getAs("date"),
                        row.getAs("msisdn"),
                        row.getAs("leave_base"),
                        leaveDistrictCode,
                        row.getAs("leave_lng"),
                        row.getAs("leave_lat"),
                        row.getAs("arrive_base"),
                        arriveDistrictCode,
                        row.getAs("arrive_lng"),
                        row.getAs("arrive_lat"),
                        row.getAs("leave_time"),
                        row.getAs("arrive_time"),
                        row.getAs("linked_distance"),
                        row.getAs("max_speed"),
                        row.getAs("cov_speed"),
                        row.getAs("distance"),
                        row.getAs("move_time")
                }, ODSchemaProvider.OD_DISTRICT_SCHEMA);
            }
        });
        return sqlContext.createDataFrame(odRdd, ODSchemaProvider.OD_DISTRICT_SCHEMA)
                .filter(col("leave_district").notEqual(0).and(col("arrive_district").notEqual(0)));

    }


    /**
     * 获取全省OD中，出现在目标区县中手机号码的OD信息，包括目标区县的OD
     * @param odDf 全省OD数据
     * @return 全省OD数据中出现在目标区县的手机号码的OD数据
     */
    public DataFrame provinceResultOd(DataFrame odDf) {
        DataFrame currentDistrictDf = odDf.filter(
                col("leave_district").equalTo(params.getDistrictCode()).and(col("arrive_district").equalTo(params.getDistrictCode()))
        );
        List<Row> msisdnRowList ;
        //找出逗留时间满足要求的od
        final Integer odMode = params.getOdMode();
        if(odMode == 0) {//只要在区县有停留点，对逗留时间无要求
            msisdnRowList = currentDistrictDf.select("msisdn").dropDuplicates().collectAsList();
        } else {//逗留时间必须满足要求
            msisdnRowList = currentDistrictDf.groupBy("msisdn")
                    .agg(sum("move_time").as("sumMoveTime"))
                    .filter(col("sumMoveTime").geq(DISTRICT_STAY_MINUTE))
                    .select("msisdn").dropDuplicates().collectAsList();
        }
        List<String> msisdnList = new ArrayList<>(msisdnRowList.size());
        for (Row row : msisdnRowList) {
            msisdnList.add(row.getAs("msisdn").toString());
        }
        final Broadcast<List<String>> msisdnBroadcast = sparkContext.broadcast(msisdnList);
        JavaRDD<Row> resultOdRdd = odDf.javaRDD().filter(new Function<Row, Boolean>() {
            final List<String> msisdnList = msisdnBroadcast.getValue();
            @Override
            public Boolean call(Row row) throws Exception {
                return msisdnList.contains(row.getAs("msisdn"));
            }
        });
        return sqlContext.createDataFrame(resultOdRdd, ODSchemaProvider.OD_DISTRICT_SCHEMA);
    }






}
