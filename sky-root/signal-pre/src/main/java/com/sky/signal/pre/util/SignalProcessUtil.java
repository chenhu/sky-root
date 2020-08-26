package com.sky.signal.pre.util;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.odAnalyze.ODSchemaProvider;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
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
    // 位移点
    public static final byte MOVE_POINT = 0;
    // 可能停留点
    public static final byte UNCERTAIN_POINT = 1;
    // 停留点
    public static final byte STAY_POINT = 2;

    /************
     * 停留点类型参数
     ***************/
    //40分钟
    private static final int STAY_TIME_MAX = 40 * 60;
    //10分钟
    private static final int STAY_TIME_MIN = 10 * 60;
    //8KM/h
    private static final int MOVE_SPEED = 8;

    /**
     * 手机信令DataFrame转化为JavaPairRDD
     *
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

    public static Row getNewRowWithStayPoint(Row prior, Row current,
                                             Timestamp startTime, Timestamp
                                                     lastTime) {
        int distance = 0;
        int moveTime = (int) (lastTime.getTime() - startTime.getTime()) / 1000;
        double speed = 0d;
        if (prior != current && current != null) {
            //基站与下一基站距离
            distance = MapUtil.getDistance((double) current.getAs("lng"),
                    (double) current.getAs("lat"), (double) prior.getAs
                            ("lng"), (double) prior.getAs("lat"));
            //移动到下一基站时间 = 下一基站startTime - 基站startTime
            moveTime = Math.abs(Seconds.secondsBetween(new DateTime(current
                    .getAs("begin_time")), new DateTime(prior.getAs
                    ("begin_time"))).getSeconds());
            //基站移动到下一基站速度
            speed = MapUtil.formatDecimal(moveTime == 0 ? 0 : (double)
                    distance / moveTime * 3.6, 2);
        }
        String base = prior.getAs("base");
        byte pointType = getPointType(base, moveTime, speed);
        return new GenericRowWithSchema(new Object[]{prior.getAs("date"),
                prior.getAs("msisdn"), prior.getAs("base"), prior.getAs
                ("lng"), prior.getAs("lat"), startTime, lastTime, distance,
                moveTime, speed, pointType}, ODSchemaProvider.TRACE_SCHEMA);
    }


    private static byte getPointType(String base, int moveTime, double speed) {
        byte pointType = MOVE_POINT;
        ParamProperties paramProperties = ProjectApplicationContext.getBean(ParamProperties.class);
        if (moveTime >= STAY_TIME_MIN && moveTime < STAY_TIME_MAX &&
                speed < MOVE_SPEED) {
            pointType = UNCERTAIN_POINT;
        } else if (moveTime >= STAY_TIME_MAX) {
            pointType = STAY_POINT;
        }

        return pointType;
    }


}
