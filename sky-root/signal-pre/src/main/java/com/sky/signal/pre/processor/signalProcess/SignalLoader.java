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

    public DataFrame getOrginalSignal(JavaRDD<String> lines) {
        JavaRDD<Row> rdd = lines.flatMap(new FlatMapFunction<String, Row>() {
            @Override
            public List<Row> call(String line) throws Exception {
                String[] prop = line.split("\0");
                String[] props = prop[1].split("\1");
                List<Row> rows = new ArrayList<>();
                for (int i = 0; i < props.length; i++) {
                    Integer date = 0;
                    String strip = props[i];
                    if (!Strings.isNullOrEmpty(strip)) {
                        String[] strips = strip.split("\\|");
                        if (strips.length >= 18) {
                            try {
                                date = Integer.valueOf(DateTime.parse(strips[5], FORMATTER).toString(FORMATTED));
                            } catch (Exception e) {
                                e.printStackTrace();

                            }
                        }
                        Row row = RowFactory.create(date, strip);
                        rows.add(row);
                    }
                }
                return rows;
            }
        });

        DataFrame orginalDf = sqlContext.createDataFrame(rdd, SignalSchemaProvider.SIGNAL_SCHEMA_ORGINAL);

        return orginalDf;
    }

    /**
     * Parquet格式轨迹解析
     * @param df
     * @return
     */
    public DataFrame mergeCell(DataFrame df) {
        final Integer  cityCode = Integer.valueOf(params.getCityCode());
        JavaRDD<Row> rdd = df.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String startTime = row.getAs("start_time").toString();
                String endTime = row.getAs("end_time").toString();
                Integer date = Integer.valueOf(DateTime.parse(startTime, FORMATTER).toString(FORMATTED));
                String msisdn = row.getAs("msisdn");
                Integer city_code = cityCode;
                Integer region = Integer.valueOf(row.getAs("reg_city").toString());
                Integer tac = Integer.valueOf(row.getAs("lac").toString());
                Long cell = Long.valueOf(row.getAs("start_ci").toString());
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
                    base = cellRow.getString(3);
                    lng = cellRow.getDouble(4);
                    lat = cellRow.getDouble(5);
                }

                Row track = RowFactory.create(date, msisdn, region, city_code, tac, cell, base, lng, lat, begin_time, end_time);
                return track;
            }
        });

        df = sqlContext.createDataFrame(rdd, SignalSchemaProvider.SIGNAL_SCHEMA_BASE);

        //过滤手机号码/基站不为0的数据,并删除重复手机信令数据
        DataFrame validDf = df.filter(col("msisdn").notEqual("null").and(col("base").notEqual("null")).and(col("lng").notEqual(0)).and(col("lat")
                .notEqual(0))).dropDuplicates(new String[]{"msisdn", "begin_time"});
        return validDf;
    }

    /**
     * 文本格式轨迹解析
     * @param lines
     * @return
     */
    public DataFrame mergeCell(JavaRDD<String> lines) {
        JavaRDD<Row> rdd = lines.flatMap(new FlatMapFunction<String, Row>() {
            @Override
            public List<Row> call(String line) throws Exception {
                String[] prop = line.split("\0");
                String[] props = prop[1].split("\1");
                List<Row> rows = new ArrayList<>();
                for (int i = 0; i < props.length; i++) {
                    Integer date = 0;
                    String msisdn = null;
                    Integer region = 0;
                    Integer city_code = 0;
                    Integer tac = 0;
                    Long cell = 0l;
                    String base = null;
                    double lng = 0d;
                    double lat = 0d;
                    Timestamp begin_time = null;
                    Timestamp end_time = null;
                    String strip = props[i];
                    if (!Strings.isNullOrEmpty(strip)) {
                        String[] strips = strip.split("\\|", -1);
                        if (strips.length >= 18) {
                            try {
                                date = Integer.valueOf(DateTime.parse(strips[5], FORMATTER).toString(FORMATTED));
                                msisdn = strips[0];
                                tac = Integer.valueOf(strips[14]);
                                cell = Long.valueOf(strips[6]);
                                begin_time = new Timestamp(DateTime.parse(strips[5], FORMATTER).getMillis());
                                end_time = new Timestamp(DateTime.parse(strips[9], FORMATTER).getMillis());
                                city_code = Integer.valueOf(strips[2]);
                                region = Integer.valueOf(strips[3]);
                            } catch (Exception e) {
                                e.printStackTrace();

                            }
                            //根据tac/cell查找基站信息
                            Row cellRow = cellVar.value().get(tac.toString() + '|' + cell.toString());
                            if (cellRow == null) {
                                //Do nothing
                            } else {
                                city_code = cellRow.getInt(0);
                                base = cellRow.getString(3);
                                lng = cellRow.getDouble(4);
                                lat = cellRow.getDouble(5);
                            }
                        }
                    }
                    Row row = RowFactory.create(date, msisdn, region, city_code, tac, cell, base, lng, lat, begin_time, end_time);
                    rows.add(row);

                }
                return rows;
            }
        });

        DataFrame df = sqlContext.createDataFrame(rdd, SignalSchemaProvider.SIGNAL_SCHEMA_BASE);

        //过滤手机号码/基站不为0的数据,并删除重复手机信令数据
        DataFrame validDf = df.filter(col("msisdn").notEqual("null").and(col("base").notEqual("null")).and(col("lng").notEqual(0)).and(col("lat")
                .notEqual(0))).dropDuplicates(new String[]{"msisdn", "begin_time"});
        return validDf;
    }

    /**
     * description: 合并用户户籍信息，性别 ，年龄到信令
     * param: [rdd]
     * return: org.apache.spark.api.java.JavaRDD<org.apache.spark.sql.Row>
     **/
    public JavaRDD<Row> mergeCRM(JavaRDD<Row> rdd) {
        if (this.crmVar != null) {
            //根据msisdn查找用户信息
            JavaRDD<Row> resultRDD = rdd.map(new Function<Row, Row>() {
                @Override
                public Row call(Row row) throws Exception {
                    String msisdn = row.getAs("msisdn");
                    Row userRow = crmVar.value().get(msisdn);
                    Short age = -1, sex = -1;
                    // 没有匹配到的msisdn信息，户籍所在地为0
                    Integer cen_region = 0;
                    if (userRow == null) {
                        //Do nothing
                    } else {
                        cen_region = userRow.getInt(3);
                        age = userRow.getShort(2);
                        sex = userRow.getShort(1);
                    }
                    return RowFactory.create(row.getAs("date"), row.getAs("msisdn"), row.getAs("region"), row.getAs("city_code"),
                            cen_region, sex, age, row.getAs("tac"), row.getAs("cell"), row.getAs("base"), row.getAs("lng"), row.getAs
                                    ("lat"),
                            row.getAs("begin_time"), row.getAs("last_time"), row.getAs("distance"), row.getAs("move_time"), row.getAs
                                    ("speed"));
                }
            });
            return resultRDD;
        }
        return null;
    }

    /**
     * description: 合并用户户籍信息，性别 ，年龄到信令
     * param: [rdd]
     * return: org.apache.spark.api.java.JavaRDD<org.apache.spark.sql.Row>
     **/
    public JavaRDD<Row> mergeCRM1(JavaRDD<Row> rdd) {
        if (this.crmVar != null) {
            //根据msisdn查找用户信息
            JavaRDD<Row> resultRDD = rdd.map(new Function<Row, Row>() {
                @Override
                public Row call(Row row) throws Exception {
                    String msisdn = row.getAs("msisdn");
                    Row userRow = crmVar.value().get(msisdn);
                    Short age = -1, sex = -1;
                    // 没有匹配到的msisdn信息，户籍所在地为0
                    Integer cen_region = 0;
                    if (userRow == null) {
                        //Do nothing
                    } else {
                        cen_region = userRow.getInt(3);
                        age = userRow.getShort(2);
                        sex = userRow.getShort(1);
                    }
                    DateTime begin_time = new DateTime(row.getAs("begin_time"));
                    DateTime last_time = new DateTime(row.getAs("last_time"));
                    LocalDate date1 = begin_time.toLocalDate();
                    LocalDate date2 = last_time.toLocalDate();
                    String dateStr = row.getAs("date").toString();
                    LocalDate date = new LocalDate(Integer.valueOf(dateStr.substring(0,4)) , Integer.valueOf(dateStr.substring(4,6)), Integer.valueOf(dateStr.substring(6,8)));
                    int dateCount1 = 0;
                    int dateCount2 = 0;
                    if(!date.equals(date1) || !date.equals(date2)) {
                        dateCount1 = 1;
                    }
                    if(!date1.equals(date2)) {
                        dateCount2 = 1;
                    }
                    return RowFactory.create(row.getAs("date"), row.getAs("msisdn"), row.getAs("region"), row.getAs("city_code"),
                            cen_region, sex, age, row.getAs("tac"), row.getAs("cell"), row.getAs("base"), row.getAs("lng"), row.getAs
                                    ("lat"), row.getAs("begin_time"), row.getAs("last_time"), dateCount1, dateCount2);
                }
            });
            return resultRDD;
        }
        return null;
    }

    /**
     * description: 合并用户归属地信息到信令，如果是外省的用户，则用省份代码替换归属地
     * param: [rdd]
     * return: org.apache.spark.api.java.JavaRDD<org.apache.spark.sql.Row>
     **/
    public JavaRDD<Row> mergeAttribution(JavaRDD<Row> rdd) {
        if (this.regionVar != null) {
            JavaRDD<Row> resultRDD = rdd.map(new Function<Row, Row>() {
                @Override
                public Row call(Row row) throws Exception {
                    int region = row.getAs("region");
                    //根据region查找归属地信息
                    Row regionRow = regionVar.value().get(region);
                    if (regionRow == null) {
                        //Do nothing
                    } else {
                        region = regionRow.getInt(1);
                    }
                    return RowFactory.create(row.getAs("date"), row.getAs("msisdn"), region, row.getAs("city_code"),
                            row.getAs("cen_region"), row.getAs("sex"), row.getAs("age"), row.getAs("tac"), row.getAs("cell"),
                            row.getAs("base"), row.getAs("lng"), row.getAs("lat"), row.getAs("begin_time"), row.getAs("last_time"),
                            row.getAs("distance"), row.getAs("move_time"), row.getAs("speed"));
                }
            });
            return resultRDD;
        }
        return null;
    }

    /**
     * description: 如果分析的数据限定区域，则根据区域文件，把区域合并到信令
     * param: [rdd]
     * return: org.apache.spark.api.java.JavaRDD<org.apache.spark.sql.Row>
     **/
    public JavaRDD<Row> mergeArea(JavaRDD<Row> rdd) {
        if (this.areaVar != null) {
            JavaRDD<Row> resultRDD = rdd.map(new Function<Row, Row>() {
                @Override
                public Row call(Row row) throws Exception {
                    Integer tac = row.getAs("tac");
                    Long cell = row.getAs("cell");
                    Short area = -1;
                    //根据tac/cell查找区域信息
                    if (areaVar != null) {
                        Row areaRow = areaVar.value().get(tac.toString() + '|' + cell.toString());
                        if (areaRow == null) {
                            //Do nothing
                        } else {
                            area = areaRow.getShort(2);
                        }
                    }
                    return RowFactory.create(row.getAs("date"), row.getAs("msisdn"), row.getAs("region"), row.getAs("city_code"),
                            row.getAs("cen_region"), row.getAs("sex"), row.getAs("age"), row.getAs("tac"), row.getAs("cell"),
                            row.getAs("base"), area, row.getAs("lng"), row.getAs("lat"), row.getAs("begin_time"), row.getAs("last_time"),
                            row.getAs("distance"), row.getAs("move_time"), row.getAs("speed"));
                }
            });
            return resultRDD;
        }
        return null;
    }

    public DataFrame load(String validSignalFile) {
        DataFrame df = FileUtil.readFile(FileUtil.FileType.CSV, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA, validSignalFile)
                .repartition(params.getPartitions());
        return df;
    }

}
