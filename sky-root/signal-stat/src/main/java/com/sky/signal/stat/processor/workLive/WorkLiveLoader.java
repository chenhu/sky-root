package com.sky.signal.stat.processor.workLive;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.util.FileUtil;
import com.sky.signal.stat.util.TransformFunction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

/**
 * 加载职住分析的数据
 */
@Service("workLiveLoader")
public class WorkLiveLoader implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(WorkLiveLoader.class);
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SQLContext sqlContext;


    public DataFrame load(String workLiveFile) {
        DataFrame df = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.WORK_LIVE_SCHEMA, workLiveFile);

        JavaRDD<Row> rdd = df.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                String live_base = row.getAs("live_base");
                String work_base = row.getAs("work_base");
                // 归属地是否是江苏省，1: 是; 0: 否
                Integer region = row.getAs("region");
                if (region == null) {
                    region = 0;
                }
                Integer jsRegion = TransformFunction.transformJsRegion(region);
                // 性别
                Short sex = TransformFunction.transformSexClass((Short) row.getAs("sex"), region);
                // 年龄分类
                Integer ageClass = TransformFunction.transformAgeClass((Short) row.getAs("age"), region);

                String cenRegion = TransformFunction.transformCenRegion((Integer) row.getAs("cen_region"));
                // 分析时间范围内每日平均逗留时间分类
                Double stayTime = row.getAs("stay_time");
                if (stayTime == null) {
                    stayTime = 0d;
                }
                Integer stayTimeClass = TransformFunction.transformStayTimeClass(stayTime);

                Long uld = 0l;
                Long uwd = 0l;
                Long on_lsd = 0l;
                Long on_wsd = 0l;
                if (live_base == null) {
                    live_base = region.toString();
                } else {
                    uld = row.getAs("uld");
                    on_lsd = row.getAs("on_lsd");
                }
                if (work_base == null) {
                    work_base = region.toString();
                } else {
                    uwd = row.getAs("uwd");
                    on_wsd = row.getAs("on_wsd");
                }

                Double sum_time = row.getAs("stay_time");
                Integer person_class = TransformFunction.transformPersonClass(uld, sum_time);


                return RowFactory.create(row.getAs("msisdn"), region, jsRegion, cenRegion, sex, row.getAs("age"), ageClass,
                        row.getAs("stay_time"), stayTimeClass, row.getAs("exists_days"), live_base, row.getAs("live_lng"), row.getAs("live_lat"),
                        on_lsd, uld, work_base, row.getAs("work_lng"), row.getAs("work_lat"), on_wsd, uwd, person_class);
            }

        });
        df = sqlContext.createDataFrame(rdd, LiveWorkSchemaProvider.WORK_LIVE_STAT_SCHEMA);
        return df;
    }
}
