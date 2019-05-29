package com.sky.signal.pre.processor.dataSelector;

import com.google.common.base.Strings;
import com.sky.signal.pre.config.ParamProperties;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.Map;

/**
* description: 从指定路径的原始信令中筛选指定小区的信令数据
* param:
* return:
**/
@Service("selectSignalData")
public class SelectSignalData implements Serializable {

    @Autowired
    private transient JavaSparkContext sparkContext;

    @Autowired
    private transient ParamProperties params;

    public JavaRDD<String> process(String path,final Broadcast<Map<String, Row>> areaVar) {
        //获取原始数据
        JavaRDD<String> lines = sparkContext.textFile(path,params.getPartitions());
        //过滤指定区域的信令数据
        JavaRDD<String> rdd=lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String s) throws Exception {
               try{
                   String[] prop = s.split("\0");
                   String[] props = prop[1].split("\1");
                   for (int i = 0; i < props.length; i++) {
                       Integer tac = 0;
                       Long cell = 0l;
                       String strip = props[i];
                       if (!Strings.isNullOrEmpty(strip)) {
                           String[] strips = strip.split("\\|");
                           if (strips.length >= 9) {
                               try {
                                   tac = Integer.valueOf(strips[3]);
                                   cell = Long.valueOf(strips[4]);
                               } catch (Exception e) {

                               }
                               //根据tac/cell查找基站信息
                               Row areaRow = areaVar.value().get(tac.toString() + '|' + cell.toString());
                               if (areaRow == null) {
                                   //Do nothing
                               } else {
                                   return true;
                               }
                           }
                       }
                   }
                   return false;
               }catch (Exception e){
                   return false;
               }
            }
        });
      return rdd;
    }
}
