package com.sky.signal.population.processor;

import com.google.common.base.Strings;
import com.google.common.collect.Ordering;
import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.util.FileUtil;
import com.sky.signal.population.util.GeoUtil;
import com.sky.signal.population.util.ProfileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * 基站数据处理
 * 1. 普通意义上基站预处理
 * 2. 特定区域内基站预处理 --add by chenhu 2020/05/23
 * <p>
 * 同时处理两种基站数据，当文件存在的时候，则认为需要处理
 */
@Component
public class CellProcess implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(CellProcess
            .class);
    @Autowired
    private transient JavaSparkContext sparkContext;

    @Autowired
    private transient SQLContext sqlContext;

    @Autowired
    private transient ParamProperties params;

    /**
     * 处理基站文件
     *
     * @return
     */
    public DataFrame process() {
        return this.processDemoFile(params.getCellFile(), params
                .getValidProvinceCellPath());
    }

    private DataFrame processBaseFile(String filePath, String dir) {
        JavaRDD<String> lines = sparkContext.textFile(filePath);
        JavaRDD<Row> rdd = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                Integer city_code = 0;
                Integer tac = 0;
                Long cell = 0L;
                Double lng = 0d;
                Double lat = 0d;
                //基站名称
                String name = "";
                //基站所在区县编码
                String district = "";
                //方位角
                Integer direction = 0;
                String geoHash = null;
                if (!Strings.isNullOrEmpty(line)) {
                    String[] props = line.split(",");
                    if (props.length >= 4) {
                        try {
                            city_code = Integer.valueOf(props[0]);
                            district = props[1];
                            tac = Integer.valueOf(props[2]);
                            cell = Long.valueOf(props[3]);
                            lng = Double.valueOf(props[4]);
                            lat = Double.valueOf(props[5]);
                            direction = Integer.valueOf(props[6]);
                            name = props[7];
                            geoHash = GeoUtil.geo(lat, lng);
                        } catch (Exception e) {
                            city_code = 0;
                            district = "";
                            tac = 0;
                            cell = 0L;
                            lng = 0d;
                            lat = 0d;
                            direction = 0;
                            name = "";
                            geoHash = "";
                        }
                    }
                }
                return RowFactory.create(city_code, district, tac, cell, lng,
                        lat, direction, name, geoHash);
            }
        });

        DataFrame df = sqlContext.createDataFrame(rdd, CellSchemaProvider
                .CELL_SCHEMA_OLD);
        df = df.filter(col("tac").notEqual(0).and(col("cell").notEqual(0)));

        //对数据按照cell字段作升序排序，这样能保证取到最小的cell值为基站编码一部分
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf
                (new com.google.common.base.Function<Row, Long>() {
                    @Override
                    public Long apply(Row row) {
                        return Long.valueOf(row.getAs("cell").toString());
                    }
                });
        List<Row> rows = ordering.sortedCopy(df.collectAsList());

        //位置相同的基站使用同一个基站编号base
        List<Row> newRows = new ArrayList<>();
        Map<String, String> cellMap = new HashMap<>();
        for (Row row : rows) {
            String base = null;
            String position = String.format("%.6f|%.6f", row.getAs("lng"),
                    row.getAs("lat"));
            if (cellMap.containsKey(position)) {
                base = cellMap.get(position);
            } else {
                base = row.getAs("cell").toString();
                cellMap.put(position, base);
            }
            base = row.getAs("tac").toString() + '|' + base;
            newRows.add(RowFactory.create(row.getAs("city_code"), row.getAs
                            ("district"), row.getAs("tac"), row.getAs("cell"), base,
                    row.getAs("lng"), row.getAs("lat"), row.getAs
                            ("direction"), row.getAs("name"), row.getAs
                            ("geohash")));
        }

        df = sqlContext.createDataFrame(newRows, CellSchemaProvider
                .CELL_SCHEMA).cache();

        int partitions = 1;
        if (!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
        FileUtil.saveFile(df.repartition(partitions), FileUtil.FileType.CSV,
                dir);
        // 生成geohash和经纬度对应表
        FileUtil.saveFile(df.select("lng", "lat", "geohash").dropDuplicates()
                .repartition(1), FileUtil.FileType.CSV, params
                .getGeoHashFilePath());
        return df;
    }

    private DataFrame processDemoFile(String filePath, String dir) {
        JavaRDD<String> lines = sparkContext.textFile(filePath).repartition(20);
        JavaRDD<Row> rdd = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                Integer city_code = 0;
                Integer tac = 0;
                Long cell = 0L;
                Double lng = 0d;
                Double lat = 0d;
                //基站名称
                String name = "";
                //基站所在区县编码
                String district = "";
                //街道编码
                String townId = "";
                //方位角
                Integer direction = 0;
                String geoHash = null;
                //----------------
                String netId = "", netType = "", boundaryCellType = "";
                //--------------
                if (!Strings.isNullOrEmpty(line)) {
                    String[] props = line.split("\01");
                    if (props.length >= 4) {
                        try {
                            city_code = Integer.valueOf(props[1]);
                            district = props[2];
                            townId = props[3];
                            tac = Integer.valueOf(props[8]);
                            cell = Long.valueOf(props[9]);
                            lng = Double.valueOf(props[6]);
                            lat = Double.valueOf(props[7]);
                            netId = props[10];
                            netType = props[11];
                            boundaryCellType = props[12];
                            geoHash = GeoUtil.geo(lat, lng);
                        } catch (Exception e) {
                            city_code = 0;
                            district = "";
                            tac = 0;
                            cell = 0L;
                            lng = 0d;
                            lat = 0d;
                            direction = 0;
                            name = "";
                            geoHash = "";
                        }
                    }
                }
                return RowFactory.create(city_code, district, townId, tac, cell, lng,
                        lat, direction, name, geoHash, netId, netType, boundaryCellType);
            }
        });

        DataFrame df = sqlContext.createDataFrame(rdd, CellSchemaProvider
                .CELL_SCHEMA_FULL);
        df = df.filter(col("tac").notEqual(0).and(col("cell").notEqual(0)));

        //对数据按照cell字段作升序排序，这样能保证取到最小的cell值为基站编码一部分
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf
                (new com.google.common.base.Function<Row, Long>() {
                    @Override
                    public Long apply(Row row) {
                        return Long.valueOf(row.getAs("cell").toString());
                    }
                });
        List<Row> rows = ordering.sortedCopy(df.collectAsList());

        //位置相同的基站使用同一个基站编号base
        List<Row> newRows = new ArrayList<>();
        Map<String, String> cellMap = new HashMap<>();
        for (Row row : rows) {
            String base = null;
            String position = String.format("%.6f|%.6f", row.getAs("lng"),
                    row.getAs("lat"));
            if (cellMap.containsKey(position)) {
                base = cellMap.get(position);
            } else {
                base = row.getAs("cell").toString();
                cellMap.put(position, base);
            }
            base = row.getAs("tac").toString() + '|' + base;
            newRows.add(RowFactory.create(row.getAs("city_code"), row.getAs
                            ("district"), row.getAs("townId"), row.getAs("tac"), row.getAs("cell"), base,
                    row.getAs("lng"), row.getAs("lat"), row.getAs
                            ("direction"), row.getAs("name"), row.getAs
                            ("geohash"),
                    row.getAs("netId"), row.getAs("netType"), row.getAs("boundaryCellType")));
        }

        df = sqlContext.createDataFrame(newRows, CellSchemaProvider
                .CELL_SCHEMA_NEW).cache();

        int partitions = 1;
        if (!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
        FileUtil.saveFile(df.repartition(partitions), FileUtil.FileType.CSV,
                dir);
        // 生成geohash和经纬度对应表
        FileUtil.saveFile(df.select("lng", "lat", "geohash").dropDuplicates()
                .repartition(1), FileUtil.FileType.CSV, params
                .getGeoHashFilePath());
        return df;
    }
}
