package com.sky.signal.population.util;

import com.sky.signal.population.config.ParamProperties;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.joda.time.DateTime;
import org.joda.time.Seconds;
import scala.Tuple2;
import scala.Tuple3;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Chenhu on 2020/5/25.
 * 用于把信令处理中常用的代码块统一管理，进行复用
 */
public class SignalProcessUtil {
    /**
     * 手机信令DataFrame转化为JavaPairRDD
     *
     * @param validSignalDf 信令df
     * @param params 参数配置工具类
     * @return
     */
    public static JavaPairRDD<String, List<Row>> keyPairByMsisdn
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

    /**
     * 手机信令DataFrame转化为JavaPairRDD，key为区县编码，value为区县区县信令
     *
     * @param signalDf 需要转换的信令
     * @param params 参数配置工具类
     * @return
     */
    public static JavaPairRDD<String, List<Row>> keyPairByDistrictCode
    (DataFrame signalDf, ParamProperties params) {
        JavaPairRDD<String, Row> signalRDD = signalDf.javaRDD()
                .mapToPair(new PairFunction<Row, String, Row>() {
                    public Tuple2<String, Row> call(Row row) throws Exception {
                        String district = row.getAs("district");
                        return new Tuple2<>(district, row);
                    }
                });

        //对数据进行分组，相同区县放到一个集合里面
        List<Row> rows = new ArrayList<>();
        return signalRDD.aggregateByKey(rows, params.getPartitions(),
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


    /**
     * 计算两条信令之间的 distance move_time speed
     * 如果下一条信令为空，则默认返回距离和速度为0
     *
     * @param current   当前信令
     * @param next      按照开始时间排序后的下一条信令
     * @param beginTime 开始时间
     * @param lastTime  离开时间
     * @return 距离（米）、逗留时间（秒）、速度（千米/小时）的三元组
     */
    public static Tuple3<Integer, Integer, Double> getDistanceMovetimeSpeed
    (Row current, Row next, Timestamp beginTime, Timestamp lastTime) {
        int distance = 0;
        int moveTime = (int) (lastTime.getTime() - beginTime.getTime()) / 1000;
        double speed = 0d;
        if (current != null && next != null) {
            //基站与下一基站距离
            distance = MapUtil.getDistance((double) next.getAs("lng"),
                    (double) next.getAs("lat"), (double) current.getAs("lng")
                    , (double) current.getAs("lat"));
            //基站移动到下一基站速度
            speed = MapUtil.formatDecimal(moveTime == 0 ? 0 : distance /
                    moveTime * 3.6, 2);
        }
        return new Tuple3<>(distance, moveTime, speed);
    }

    /**
     * description: 计算两个时间直接的秒数差
     * param: [t1, t2]
     * return: int
     **/
    public static int getTimeDiff(Timestamp t1, Timestamp t2) {
        // t2 - t1
        return Seconds.secondsBetween(new DateTime(t1), new DateTime(t2))
                .getSeconds();
    }


}