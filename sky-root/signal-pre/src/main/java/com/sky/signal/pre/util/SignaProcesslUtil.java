package com.sky.signal.pre.util;

import com.sky.signal.pre.config.ParamProperties;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Chenhu on 2020/5/25.
 * 用于把信令处理中常用的代码块统一管理，进行复用
 */
public class SignaProcesslUtil {

    /**
     * 手机信令DataFrame转化为JavaPairRDD
     * @param validSignalDf
     * @param params
     * @return
     */
    public static JavaPairRDD<String, List<Row>> signalToJavaPairRDD
            (DataFrame validSignalDf, ParamProperties params) {
        JavaPairRDD<String, Row> validSignalRDD = validSignalDf.javaRDD()
                .mapToPair(new PairFunction<Row, String, Row>() {
            public Tuple2<String, Row> call(Row row) throws Exception {
                String msisdn = row.getAs("msisdn");
                return new Tuple2<>(msisdn, row);
            }
        });

        //对数据进行分组，相同手机号放到一个集合里面
        List<Row> rows = new ArrayList<>();
        return validSignalRDD.aggregateByKey(rows, params.getPartitions(),
                new Function2<List<Row>, Row, List<Row>>() {
            @Override
            public List<Row> call(List<Row> rows1, Row row) throws Exception {
                rows1.add(row);
                return rows1;
            }
        }, new Function2<List<Row>, List<Row>, List<Row>>() {
            @Override
            public List<Row> call(List<Row> rows1, List<Row> rows2) throws
                    Exception {
                rows1.addAll(rows2);
                return rows1;
            }
        });
    }
}
