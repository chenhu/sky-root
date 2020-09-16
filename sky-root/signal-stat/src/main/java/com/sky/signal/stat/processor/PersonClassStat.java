package com.sky.signal.stat.processor;

import com.google.common.collect.Lists;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.util.ChangshuPersonClassification;
import com.sky.signal.stat.util.FileUtil;
import com.sky.signal.stat.util.TransformFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

import static org.apache.spark.sql.functions.sum;

/**
 * description: 按照人口分类统计人口数量
 * param:
 * return:
 **/
@Service("personClassStat")
public class PersonClassStat implements Serializable {
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;

    private static final StructType SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("exists_days", DataTypes.LongType, false),
            DataTypes.createStructField("stay_time_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("live_base", DataTypes.StringType, true),
            DataTypes.createStructField("on_lsd", DataTypes.LongType, true),
            DataTypes.createStructField("uld", DataTypes.LongType, true),
            DataTypes.createStructField("work_base", DataTypes.StringType, true),
            DataTypes.createStructField("on_wsd", DataTypes.LongType, true),
            DataTypes.createStructField("uwd", DataTypes.LongType, true),
            //DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("age_class", DataTypes.ShortType, false),
            DataTypes.createStructField("region", DataTypes.IntegerType, false),
            DataTypes.createStructField("cen_region", DataTypes.StringType, false),
            DataTypes.createStructField("peo_num", DataTypes.LongType, true)
            ));
    private static final StructType STAT_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("uld", DataTypes.LongType, false),
            DataTypes.createStructField("uld_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("stay_time_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("person_class", DataTypes.IntegerType, false),
            DataTypes.createStructField("peo_num", DataTypes.LongType, false)
    ));

    public DataFrame process() {
        DataFrame workLiveStatDf  = FileUtil.readFile(FileUtil.FileType.CSV, SCHEMA, params.getWorkLiveStatSavePath());
        JavaRDD<Row> rdd = workLiveStatDf.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                Long uld = 0l;
                if(row.getAs("uld") != null) {
                    uld = (Long) row.getAs("uld");
                }
                Integer uld_class = TransformFunction.transformULDClass(uld);
                Integer stay_time_class = (Integer) row.getAs("stay_time_class");
                Integer person_class = ChangshuPersonClassification.classify(uld, stay_time_class);
                return RowFactory.create(uld, uld_class, stay_time_class,person_class, row.getAs("peo_num"));
            }

        });
        workLiveStatDf = sqlContext.createDataFrame(rdd, STAT_SCHEMA);
        workLiveStatDf.persist(StorageLevel.DISK_ONLY());

        DataFrame statDf1 = workLiveStatDf.groupBy("person_class").agg(sum("peo_num").as("peo_num")).orderBy("person_class");
        DataFrame statDf2 = workLiveStatDf.groupBy("uld_class","stay_time_class").agg(sum("peo_num").as("peo_num")).orderBy("uld_class","stay_time_class");
        FileUtil.saveFile(statDf1.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getPersonClassStat1SavePath());
        FileUtil.saveFile(statDf2.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getPersonClassStat2SavePath());
        workLiveStatDf.unpersist();
        return null;
    }
}
