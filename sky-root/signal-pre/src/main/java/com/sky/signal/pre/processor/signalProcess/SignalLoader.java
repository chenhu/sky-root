package com.sky.signal.pre.processor.signalProcess;

import com.google.common.base.Strings;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * 加载原始手机信令数据，并把城市代码、基站编号、年龄、性别、户籍所在地、区域、号码归属地信息加入到信令字段中
 */
@Component
public class SignalLoader implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SignalLoader.class);
    private static final DateTimeFormatter FORMATTER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.S");
    private static final DateTimeFormatter FORMATTED = DateTimeFormat.forPattern("yyyyMMdd");
    private Broadcast<Map<String, Row>> cellVar;
    private Broadcast<Map<String, Row>> areaVar;
    private Broadcast<Map<Integer, Row>> regionVar;
    private Broadcast<Map<String, Row>> crmVar;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;

    public SignalLoader cell(Broadcast<Map<String, Row>> cell) {
        this.cellVar = cell;
        return this;
    }

    public SignalLoader area(Broadcast<Map<String, Row>> area) {
        this.areaVar = area;
        return this;
    }

    public SignalLoader region(Broadcast<Map<Integer, Row>> region) {
        this.regionVar = region;
        return this;
    }

    public SignalLoader crm(Broadcast<Map<String, Row>> crm) {
        this.crmVar = crm;
        return this;
    }

    /**
     * Parquet格式轨迹解析
     * @param df
     * @return
     */
    public DataFrame mergeCell(DataFrame df) {
        JavaRDD<Row> rdd = df.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String startTime = row.getAs("start_time").toString();
                String endTime = row.getAs("end_time").toString();
                Integer date = Integer.valueOf(DateTime.parse(startTime, FORMATTER).toString(FORMATTED));
                String msisdn = row.getAs("msisdn").toString();
                Integer region = Integer.valueOf(row.getAs("reg_city").toString());
                Integer tac = Integer.valueOf(row.getAs("lac").toString());
                Long cell = Long.valueOf(row.getAs("start_ci").toString());
                Integer cityCode = 0;
                Integer districtCode = 0;
                String base = null;
                double lng = 0d;
                double lat = 0d;
                Timestamp begin_time = new Timestamp(DateTime.parse(startTime, FORMATTER).getMillis());
                Timestamp end_time = new Timestamp(DateTime.parse(endTime, FORMATTER).getMillis());
                //根据tac/cell查找基站信息
                Row cellRow = cellVar.value().get(tac.toString() + '|' + cell.toString());
                if (cellRow == null) {
                    //Do nothing
                } else {
                    base = cellRow.getAs("base");
                    lng = cellRow.getAs("lng");
                    lat = cellRow.getAs("lat");
                    cityCode = cellRow.getAs("city_code");
                    districtCode = cellRow.getAs("district_code");
                }

                Row track = RowFactory.create(date, msisdn, region, cityCode, districtCode, tac, cell, base, lng, lat, begin_time, end_time);
                return track;
            }
        });

        df = sqlContext.createDataFrame(rdd, SignalSchemaProvider.SIGNAL_SCHEMA_BASE);

        //过滤手机号码/基站不为0的数据,并删除重复手机信令数据
        DataFrame validDf = df.filter(col("msisdn").notEqual("null").and(col("base").notEqual("null")).and(col("lng").notEqual(0)).and(col("lat")
                .notEqual(0))).dropDuplicates(new String[]{"msisdn", "begin_time"});
        return validDf;
    }

    public DataFrame load(String validSignalFile) {
        DataFrame df = FileUtil.readFile(FileUtil.FileType.PARQUET, SignalSchemaProvider.SIGNAL_SCHEMA_BASE_1, validSignalFile)
                .repartition(params.getPartitions());
        return df;
    }

}
