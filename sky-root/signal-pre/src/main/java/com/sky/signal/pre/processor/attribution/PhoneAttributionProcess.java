package com.sky.signal.pre.processor.attribution;

import com.google.common.base.Strings;
import com.sky.signal.pre.config.ParamProperties;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/6 16:45
 * description: 加载手机号码归属地文件，处理后广播
 */
@Service("phoneAttributionProcess")
public class PhoneAttributionProcess implements Serializable {
    @Autowired
    private transient JavaSparkContext sparkContext;
    @Autowired
    private transient ParamProperties params;
    public Broadcast< Map<Integer,Row>> process() {
        JavaRDD<String> regionsRdd=sparkContext.textFile(params.getGSDZFilePath());
        JavaRDD<Row> regionRdd= regionsRdd.map(new org.apache.spark.api.java.function.Function<String,Row>() {
            @Override
            public Row call(String line) {
                Integer oldRegion=0;
                Integer region=0;
                if (!Strings.isNullOrEmpty(line)) {
                    String[] props = line.split(",");
                    if (props.length>=5) {
                        oldRegion = Integer.valueOf(props[3]);
                        region = Integer.valueOf(props[4]);
                    }
                }
                return RowFactory.create(oldRegion,region);
            }
        });
        List<Row> regionRows=regionRdd.collect();
        Map<Integer,Row> regionMap = new HashMap<>();
        for (Row row:regionRows) {
            regionMap.put(row.getInt(0),row);
        }
        final Broadcast< Map<Integer,Row>> regionVar = sparkContext.broadcast(regionMap);
        return regionVar;
    }
}
