package com.sky.signal.pre.processor.dataSelector;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

@Component
public class SelectDate implements Serializable{
    @Autowired
    private transient JavaSparkContext sparkContext;

    @Autowired
    private transient SQLContext sqlContext;

    @Autowired
    private transient ParamProperties params;

    public static final StructType CELL_SCHEMA_OLD = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("tac", DataTypes.IntegerType, false),
            DataTypes.createStructField("cell", DataTypes.LongType, false)));

    public static final StructType CELL_SCHEMA_YC = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("city_code", DataTypes.IntegerType, false),
            DataTypes.createStructField("tac", DataTypes.IntegerType, false),
            DataTypes.createStructField("cell", DataTypes.LongType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("lat", DataTypes.DoubleType, false)));

    public static final StructType CELL_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("tac", DataTypes.IntegerType, false),
            DataTypes.createStructField("cell", DataTypes.LongType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("lng", DataTypes.DoubleType, false),
            DataTypes.createStructField("lat", DataTypes.DoubleType, false)));

    private static final StructType SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("date", DataTypes.IntegerType, false),
            DataTypes.createStructField("base", DataTypes.StringType, false),
            DataTypes.createStructField("time_inter", DataTypes.ByteType, false),
            DataTypes.createStructField("region", DataTypes.IntegerType, false),
            DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("age", DataTypes.ShortType, false),
            DataTypes.createStructField("peo_num", DataTypes.LongType, false)));

    public void process(){
    //读取原始基站数据
        JavaRDD<String> lines = sparkContext.textFile(params.getSavePath()+"baseArea.csv");
        JavaRDD<Row> rdd = lines.map(new Function<String, Row>() {
            @Override
            public Row call(String line) throws Exception {
                Integer tac = 0;
                Long cell = 0L;
                if (!Strings.isNullOrEmpty(line)) {
                    String[] props = line.split(",");
                    if (props.length >= 4) {
                        try {
                            tac = Integer.valueOf(props[0]);
                            cell = Long.valueOf(props[1]);
                        }catch (Exception e){
                            tac = 0;
                            cell = 0L;
                        }
                    }
                }
                return RowFactory.create(tac, cell);
            }
        });

        DataFrame df = sqlContext.createDataFrame(rdd, CELL_SCHEMA_OLD);
        df = df.filter(col("tac").notEqual(0).and(col("cell").notEqual(0)));

        DataFrame df1= FileUtil.readFile(FileUtil.FileType.CSV,CELL_SCHEMA_YC,params.getSavePath()+"cell");

        DataFrame df2=df.join(df1,df.col("tac").equalTo(df1.col("tac")).and(df.col("cell").equalTo(df1.col("cell")))).
                select(df.col("tac"),df.col("cell"),df1.col("lng"),df1.col("lat"));

        //对原始数据进行排序
        Ordering<Row> ordering = Ordering.natural().nullsFirst().onResultOf(new com.google.common.base.Function<Row, Long>() {
            @Override
            public Long apply(Row row) {
                return row.getAs("cell");
            }
        });
        List<Row> rows = ordering.sortedCopy(df2.collectAsList());

        //位置相同的基站使用同一个基站编号base
        List<Row> newRows = new ArrayList<>();
        Map<String, String> cellMap = new HashMap<>();
            for (Row row : rows) {
            String base = null;
            String position = String.format("%.6f|%.6f", row.getDouble(2), row.getDouble(3));
            if (cellMap.containsKey(position)) {
                base = cellMap.get(position).toString();
            } else {
                base = row.getAs("cell").toString();
                cellMap.put(position, base);
            }
            base=row.getAs("tac").toString() +'|'+base;
            newRows.add(RowFactory.create( row.getInt(0), row.getLong(1),base,row.getDouble(2),row.getDouble(3)));
        }
        DataFrame df3 = sqlContext.createDataFrame(newRows, CELL_SCHEMA);
        //广播基站数据
        Row[] cellRows = df3.collect();
        Map<String, Row> cellMap1 = new HashMap<>(cellRows.length);
        for (Row row : cellRows) {
            Integer tac = row.getInt(0);
            Long cell = row.getLong(1);
            cellMap1.put(tac.toString() + '|' + cell.toString(), row);
        }
        final Broadcast<Map<String, Row>> cellVar = sparkContext.broadcast(cellMap1);

        DataFrame df4= FileUtil.readFile(FileUtil.FileType.CSV,SCHEMA,params.getSavePath()+"1.3").repartition(params.getPartitions());

        JavaRDD<Row> rdd1=df4.javaRDD().filter(new Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                String base=row.getAs("base");
                Row cellRow = cellVar.value().get(base);
                if (cellRow == null) {
                   return false;
                } else {
                   return true;
                }
            }
        });
        rdd1=rdd1.repartition(1);
       DataFrame df5 = sqlContext.createDataFrame(rdd1, SCHEMA);
        FileUtil.saveFile(df5, FileUtil.FileType.CSV,params.getSavePath()+"JLH1.3");

    }
}
