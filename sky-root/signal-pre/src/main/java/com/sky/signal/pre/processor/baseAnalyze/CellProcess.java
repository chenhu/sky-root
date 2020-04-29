package com.sky.signal.pre.processor.baseAnalyze;

import com.google.common.base.Strings;
import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.util.FileUtil;
import com.sky.signal.pre.util.GeoUtil;
import com.sky.signal.pre.util.ProfileUtil;
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
 */
@Component
public class CellProcess implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(CellProcess.class);
    @Autowired
    private transient JavaSparkContext sparkContext;

    @Autowired
    private transient SQLContext sqlContext;

    @Autowired
    private transient ParamProperties params;

    public DataFrame process() {
        //读取原始基站数据
        JavaRDD<String> lines = sparkContext.textFile(params.getCellFile());
        final Integer  cityCode = Integer.valueOf(params.getCityCode());
        JavaRDD<Row> rdd = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                Integer city_code=0;
                Integer tac = 0;
                Long cell = 0L;
                Double lng = 0d;
                Double lat = 0d;
                String geoHash = null;
                if (!Strings.isNullOrEmpty(line)) {
                    String[] props = line.split(",");
                    if (props.length >= 4) {
                        try {
                            city_code=cityCode;
                            tac = Integer.valueOf(props[0]);
                            cell = Long.valueOf(props[1]);
                            lng = Double.valueOf(props[2]);
                            lat = Double.valueOf(props[3]);
                            geoHash = GeoUtil.geo(lat, lng);
                        }catch (Exception e){
                             city_code=0;
                             tac = 0;
                             cell = 0L;
                             lng = 0d;
                             lat = 0d;
                            geoHash = "";
                        }
                    }
                }
                return RowFactory.create(city_code,tac, cell, lng, lat, geoHash);
            }
        });

        DataFrame df = sqlContext.createDataFrame(rdd, CellSchemaProvider.CELL_SCHEMA_OLD);
        df = df.filter(col("tac").notEqual(0).and(col("cell").notEqual(0)));

        //对原始数据进行排序
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Long>() {
            @Override
            public Long apply(Row row) {
                return row.getAs("cell");
            }
        });
        List<Row> rows = ordering.sortedCopy(df.collectAsList());

        //位置相同的基站使用同一个基站编号base
        List<Row> newRows = new ArrayList<>();
        Map<String, String> cellMap = new HashMap<>();
        for (Row row : rows) {
            String base = null;
            String position = String.format("%.6f|%.6f", row.getDouble(3), row.getDouble(4));
            if (cellMap.containsKey(position)) {
                base = cellMap.get(position).toString();
            } else {
                base = row.getAs("cell").toString();
                cellMap.put(position, base);
            }
            base=row.getAs("tac").toString() +'|'+base;
            newRows.add(RowFactory.create(row.getInt(0), row.getInt(1), row.getLong(2),base,row.getDouble(3),row.getDouble(4), row.getString(5)));
        }

        df = sqlContext.createDataFrame(newRows, CellSchemaProvider.CELL_SCHEMA);

        int partitions = 1;
        if(!ProfileUtil.getActiveProfile().equals("local")) {
            partitions = params.getPartitions();
        }
        FileUtil.saveFile(df.repartition(partitions), FileUtil.FileType.CSV, params.getBasePath() + "cell");
        // 生成geohash和经纬度对应表
        FileUtil.saveFile(df.select("lng","lat","geohash").dropDuplicates().repartition(1),FileUtil.FileType.CSV, params.getSavePath() + "geohash");
        return df;
    }
}
