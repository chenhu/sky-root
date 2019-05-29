package com.sky.signal.pre.processor.crmAnalyze;

import com.google.common.base.Strings;
import com.sky.signal.pre.config.ParamProperties;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
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
 * date: 2019/4/6 16:31
 * description: 手机用户的CRM信息加载和处理，并广播
 */
@Service("crmProcess")
public class CRMProcess implements Serializable {

    @Autowired
    private transient JavaSparkContext sparkContext;
    @Autowired
    private transient ParamProperties params;
    public final Broadcast<Map<String, Row>> process() {
        JavaRDD<String> usersRdd=sparkContext.textFile(params.getUserFile());
        JavaRDD<Row> userRdd= usersRdd.map(new Function<String,Row>() {
            @Override
            public Row call(String line) {
                String msisdn=null;
                Short sex=-2;
                Short age=-2;
                Integer id=-1;
                if (!Strings.isNullOrEmpty(line)) {
                    String[] props = line.split(",");
                    if(props.length>=5) {
                        try{
                            msisdn = props[0];
                            sex = Short.valueOf(props[3]);
                            age = Short.valueOf(props[2]);
                            id = Integer.valueOf(props[4]);
                        }catch (Exception e){
                            msisdn=null;
                            sex=-2;
                            age=-2;
                            id=-1;
                        }
                    }
                }
                return RowFactory.create(msisdn,sex,age,id);
            }
        });
        List<Row> userRows=userRdd.filter(new org.apache.spark.api.java.function.Function<Row, Boolean>() {
            @Override
            public Boolean call(Row row) throws Exception {
                String msisdn=row.getString(0);
                Short sex=row.getShort(1);
                Short age=row.getShort(2);
                Integer id=row.getInt(3);
                if (msisdn==null|sex==-2|age==-2|id==-1){
                    return false;
                }
                return true;
            }
        }).collect();
        Map<String, Row> UserMap = new HashMap<>(userRows.size());
        for (Row row:userRows) {
            UserMap.put(row.getString(0),row);
        }
        final Broadcast<Map<String, Row>> userVar = sparkContext.broadcast(UserMap);
        return userVar;
    }

}
