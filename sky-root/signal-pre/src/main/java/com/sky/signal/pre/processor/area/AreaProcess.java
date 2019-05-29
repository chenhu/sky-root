package com.sky.signal.pre.processor.area;

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
 * date: 2019/4/6 16:38
 * description: 有时候，需要处理的信令只需要选择一定区域范围内的。本类用来加载规定范围内的区域基站信息，
 * 并把这个区域组合成 以 base为key，tac,cell,area 为value的Map，并广播
 */
@Service("areaProcess")
public class AreaProcess implements Serializable {

    @Autowired
    private transient JavaSparkContext sparkContext;

    @Autowired
    private transient ParamProperties params;
    public final Broadcast<Map<String, Row>> process() {
         //广播区域信息
         JavaRDD<String> areasRdd=sparkContext.textFile(params.getBaseFile());
         JavaRDD<Row> areaRdd= areasRdd.map(new org.apache.spark.api.java.function.Function<String,Row>() {
        @Override
        public Row call(String line) {
        Integer tac=0;
        Long cell=0l;
        Short area=0;
        if (!Strings.isNullOrEmpty(line)) {
        String[] props = line.split(",");
        if (props.length>=4) {
        tac = Integer.valueOf(props[0]);
        cell = Long.valueOf(props[1]);
        area = Short.valueOf(props[3]);
        }
        }
        return RowFactory.create(tac,cell,area);
        }
        });
         List<Row> areaRows=areaRdd.collect();
         Map<String, Row> areaMap = new HashMap<>(areaRows.size());
         for (Row row:areaRows) {
         Integer tac = row.getInt(0);
         Long cell = row.getLong(1);
         areaMap.put(tac.toString()+'|'+cell.toString() ,row);
         }
         final Broadcast<Map<String, Row>> areaVar = sparkContext.broadcast(areaMap);
        return areaVar;
    }
}
