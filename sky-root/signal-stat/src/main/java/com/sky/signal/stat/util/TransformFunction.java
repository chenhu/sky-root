package com.sky.signal.stat.util;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.Serializable;
import java.sql.Timestamp;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/5/3 10:57
 * description: OD分析工具类
 */
public class TransformFunction implements Serializable {

    /**
     * description: 时间分割，一天时段每小时分割一次
     * param: [odDF]
     * return: org.apache.spark.api.java.JavaRDD<org.apache.spark.sql.Row>
     **/
    public static final JavaRDD<Row> transformTime1(DataFrame odDF) {
        JavaRDD<Row> odRDD = odDF.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                DateTime leaveTime = new DateTime(row.getAs("leave_time")) ;
                DateTime arriveTime = new DateTime(row.getAs("arrive_time"));
                int leaveIntervalMin =60, arriveIntervalMin = 60;
                leaveTime  = leaveTime.dayOfWeek().roundFloorCopy().plusMinutes(leaveTime.plusMinutes(-leaveTime.getMinuteOfHour()%leaveIntervalMin).getMinuteOfDay());
                arriveTime  = arriveTime.dayOfWeek().roundFloorCopy().plusMinutes(arriveTime.plusMinutes(-arriveTime.getMinuteOfHour()%arriveIntervalMin).getMinuteOfDay());

                Timestamp leaveTimestamp=new Timestamp(leaveTime.getMillis());
                Timestamp arriveTimestamp=new Timestamp(arriveTime.getMillis());
                return RowFactory.create(row.getAs("date"), row.getAs("msisdn"), row.getAs("leave_base"), row.getAs("arrive_base"),
                        leaveTimestamp, arriveTimestamp, row.getAs("distance"), row.getAs("move_time"), row.getAs("age_class"),row.getAs("sex"),
                        row.getAs("person_class"), row.getAs("trip_purpose"));
            }
        });
        return odRDD;
    }

    /**
     * description: 时间分割，一天时段每半小时分割一次
     * param: [odDF]
     * return: org.apache.spark.api.java.JavaRDD<org.apache.spark.sql.Row>
     **/
    public static final JavaRDD<Row> transformTime2(DataFrame odDF) {
        JavaRDD<Row> odRDD = odDF.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                DateTime leaveTime = new DateTime(row.getAs("leave_time")) ;
                DateTime arriveTime = new DateTime(row.getAs("arrive_time"));
                int leaveIntervalMin =30, arriveIntervalMin = 30;
                leaveTime  = leaveTime.dayOfWeek().roundFloorCopy().plusMinutes(leaveTime.plusMinutes(-leaveTime.getMinuteOfHour()%leaveIntervalMin).getMinuteOfDay());
                arriveTime  = arriveTime.dayOfWeek().roundFloorCopy().plusMinutes(arriveTime.plusMinutes(-arriveTime.getMinuteOfHour()%arriveIntervalMin).getMinuteOfDay());

                Timestamp leaveTimestamp=new Timestamp(leaveTime.getMillis());
                Timestamp arriveTimestamp=new Timestamp(arriveTime.getMillis());
                return RowFactory.create(row.getAs("date"), row.getAs("msisdn"), row.getAs("leave_geo"), row.getAs("arrive_geo"),
                        leaveTimestamp, arriveTimestamp, row.getAs("distance"), row.getAs("move_time"), row.getAs("age_class"),row.getAs("sex"),
                        row.getAs("person_class"), row.getAs("trip_purpose"));
            }
        });
        return odRDD;
    }
    /**
    * description: 时间分割，0-6点，9-16点，19-24点时间间隔为1小时；其余时段为15分钟
    * param: [odDF]
    * return: org.apache.spark.api.java.JavaRDD<org.apache.spark.sql.Row>
    **/
    public static final JavaRDD<Row> transformTime(DataFrame odDF) {
        JavaRDD<Row> odRDD = odDF.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                DateTime leaveTime = new DateTime(row.getAs("leave_time")) ;
                DateTime arriveTime = new DateTime(row.getAs("arrive_time"));
                int leaveHourOfDay = leaveTime.getHourOfDay();
                int arriveHourOfDay = arriveTime.getHourOfDay();
                int leaveIntervalMin ;
                if((leaveHourOfDay >= 0 && leaveHourOfDay < 6) || (leaveHourOfDay >= 9 && leaveHourOfDay < 16) || (leaveHourOfDay >= 19 && leaveHourOfDay < 24)) {
                    leaveIntervalMin = 60;
                } else {
                    leaveIntervalMin = 15;
                }

                int arriveIntervalMin ;
                if((arriveHourOfDay >= 0 && arriveHourOfDay < 6) || (arriveHourOfDay >= 9 && arriveHourOfDay < 16) || (arriveHourOfDay >= 19 && arriveHourOfDay < 24)) {
                    arriveIntervalMin = 60;
                } else {
                    arriveIntervalMin = 15;
                }
                leaveTime  = leaveTime.dayOfWeek().roundFloorCopy().plusMinutes(leaveTime.plusMinutes(-leaveTime.getMinuteOfHour()%leaveIntervalMin).getMinuteOfDay());
                arriveTime  = arriveTime.dayOfWeek().roundFloorCopy().plusMinutes(arriveTime.plusMinutes(-arriveTime.getMinuteOfHour()%arriveIntervalMin).getMinuteOfDay());

                Timestamp leaveTimestamp=new Timestamp(leaveTime.getMillis());
                Timestamp arriveTimestamp=new Timestamp(arriveTime.getMillis());
                return RowFactory.create(row.getAs("date"), row.getAs("msisdn"), row.getAs("leave_base"), row.getAs("arrive_base"),
                        leaveTimestamp, arriveTimestamp, row.getAs("distance"), row.getAs("move_time"), row.getAs("age_class"),row.getAs("sex"),
                        row.getAs("person_class"), row.getAs("trip_purpose"));
            }
        });
        return odRDD;
    }

    public static final JavaRDD<Row> transformTimeDistanceSpeed(DataFrame odDF) {
        JavaRDD<Row> odRDD = odDF.javaRDD().map(new Function<Row, Row>() {
            @Override
            public Row call(Row row) throws Exception {
                int avgTime = (int) row.getAs("move_time");
                int moveTimeClass = transformTime(avgTime);
                int avgDistance = (int) row.getAs("linked_distance");
                int tripDistanceClass = transformDistance(avgDistance);
                double maxSpeed = (double) row.getAs("max_speed");
                int maxSpeedClass = transformSpeed(maxSpeed);
                double covSpeed = (double) row.getAs("cov_speed");
                int covSpeedClass = transformCovSpeed(covSpeed);
                return RowFactory.create(row.getAs("date"), row.getAs("msisdn"),
                        row.getAs("linked_distance"), tripDistanceClass,maxSpeedClass, row.getAs("move_time"), moveTimeClass, covSpeedClass,
                        row.getAs("person_class"), row.getAs("age_class"), row.getAs("trip_purpose"));
            }
        });
        return odRDD;
    }
    private static int transformCovSpeed(double covSpeed) {
        int covSpeedClass;
        if(covSpeed > 0 && covSpeed <= 0.2) {
            covSpeedClass = 1;
        } else if(covSpeed > 0.2 && covSpeed <= 0.4) {
            covSpeedClass = 2;
        }else if(covSpeed > 0.4 && covSpeed <= 0.6) {
            covSpeedClass = 3;
        }else if(covSpeed > 0.6 && covSpeed <= 0.8) {
            covSpeedClass = 4;
        }else if(covSpeed > 0.8 && covSpeed <= 1) {
            covSpeedClass = 5;
        }else if(covSpeed > 1 && covSpeed <= 1.2) {
            covSpeedClass = 6;
        }else if(covSpeed > 1.2 && covSpeed <= 1.5) {
            covSpeedClass = 7;
        } else {
            covSpeedClass = 8;
        }
        return covSpeedClass;
    }

    private static int transformSpeed(double maxSpeed) {
        int maxSpeedClass;
        if(maxSpeed > 0 && maxSpeed <= 4) {
            maxSpeedClass = 1;
        } else if(maxSpeed > 4 && maxSpeed <= 8) {
            maxSpeedClass = 2;
        } else if(maxSpeed > 8 && maxSpeed <= 10) {
            maxSpeedClass = 3;
        } else if(maxSpeed > 10 && maxSpeed <= 15) {
            maxSpeedClass = 4;
        } else if(maxSpeed > 15 && maxSpeed <= 18) {
            maxSpeedClass = 5;
        } else if(maxSpeed > 18 && maxSpeed <= 20) {
            maxSpeedClass = 6;
        } else if(maxSpeed > 20 && maxSpeed <= 25) {
            maxSpeedClass = 7;
        } else if(maxSpeed > 25 && maxSpeed <= 35) {
            maxSpeedClass = 8;
        } else if(maxSpeed > 35 && maxSpeed <= 45) {
            maxSpeedClass = 9;
        } else if(maxSpeed > 45 && maxSpeed <= 60) {
            maxSpeedClass = 10;
        } else if(maxSpeed > 60 && maxSpeed <= 80) {
            maxSpeedClass = 11;
        } else if(maxSpeed > 80 && maxSpeed <= 120) {
            maxSpeedClass = 12;
        } else {
            maxSpeedClass = 13;
        }
        return maxSpeedClass;
    }

    private static int transformDistance(int avgDistance) {
        int tripDistanceClass;
        if(avgDistance > 0 && avgDistance <= 1*1000) {
            tripDistanceClass = 1;
        } else if(avgDistance > 1*1000 && avgDistance <= 2*1000) {
            tripDistanceClass = 2;
        } else if(avgDistance > 2*1000 && avgDistance <= 3*1000) {
            tripDistanceClass = 3;
        } else if(avgDistance > 3*1000 && avgDistance <= 4*1000) {
            tripDistanceClass = 4;
        } else if(avgDistance > 4*1000 && avgDistance <= 5*1000) {
            tripDistanceClass = 5;
        } else if(avgDistance > 5*1000 && avgDistance <= 6*1000) {
            tripDistanceClass = 6;
        } else if(avgDistance > 6*1000 && avgDistance <= 7*1000) {
            tripDistanceClass = 7;
        } else if(avgDistance > 7*1000 && avgDistance <= 8*1000) {
            tripDistanceClass = 8;
        } else if(avgDistance > 8*1000 && avgDistance <= 9*1000) {
            tripDistanceClass = 9;
        } else if(avgDistance > 9*1000 && avgDistance <= 10*1000) {
            tripDistanceClass = 10;
        } else if(avgDistance > 10*1000 && avgDistance <= 15*1000) {
            tripDistanceClass = 11;
        } else if(avgDistance > 15*1000 && avgDistance <= 20*1000) {
            tripDistanceClass = 12;
        } else if(avgDistance > 20*1000 && avgDistance <= 30*1000) {
            tripDistanceClass = 13;
        } else if(avgDistance > 30*1000 && avgDistance <= 45*1000) {
            tripDistanceClass = 14;
        } else {
            tripDistanceClass = 15;
        }
        return tripDistanceClass;
    }

    private static int transformTime(int avgTime) {
        int moveTimeClass;
        if(avgTime > 0 && avgTime <= 10*60) {
            moveTimeClass = 1;
        } else if(avgTime > 10*60 && avgTime <= 15*60) {
            moveTimeClass = 2;
        } else if(avgTime > 15*60 && avgTime <= 20*60) {
            moveTimeClass = 3;
        } else if(avgTime > 20*60 && avgTime <= 30*60) {
            moveTimeClass = 4;
        } else if(avgTime > 30*60 && avgTime <= 45*60) {
            moveTimeClass = 5;
        } else if(avgTime > 45*60 && avgTime <= 60*60) {
            moveTimeClass = 6;
        } else if(avgTime > 60*60 && avgTime <= 90*60) {
            moveTimeClass = 7;
        } else if(avgTime > 90*60 && avgTime <= 120*60) {
            moveTimeClass = 8;
        } else {
            moveTimeClass = 9;
        }
        return moveTimeClass;
    }
    /**
     * description: 年龄:非江苏省0，非江苏省0，1：[1， 5]，2:[6，12]，3:[13，18]，4:[19，24]，5:[25，29]，6:[30，39]，7:[40，49]，8:[50，59]，9:60及以上
     * param: [age]
     * return: java.lang.Integer
     **/
    public static final Integer transformAgeClass(Short age, Integer region) {
        Integer ageClass ;
        if (region == null || age == null || region < 100) {
            ageClass = 0;
        } else if (age >= 1 && age <= 5) {
            ageClass = 1;
        } else if (age >= 6 && age <= 12) {
            ageClass = 2;
        } else if (age >= 13 && age <= 18) {
            ageClass = 3;
        } else if (age >= 19 && age <= 24) {
            ageClass = 4;
        } else if (age >= 25 && age <= 29) {
            ageClass = 5;
        } else if (age >= 30 && age <= 39) {
            ageClass = 6;
        } else if (age >= 40 && age <= 49) {
            ageClass = 7;
        } else if (age >= 50 && age <= 59) {
            ageClass = 8;
        } else {
            ageClass = 9;
        }
        return ageClass;

    }

    /**
     * description: 户籍所在地，外省到省，江苏到市，淮安到区
     * param: [cenRegion]
     * return: java.lang.String
     **/
    public static final String transformCenRegion(Integer cenRegion) {
        String cenRegionClass;
        if(cenRegion == 0 ) {
            cenRegionClass = "NJS";
        } else {
            cenRegionClass = cenRegion.toString();
        }
        return cenRegionClass;
    }
    /**
     * description: 归属地是否是江苏省，1: 是; 0: 否
     * param: [region]
     * return: java.lang.Integer
     **/
    public static final Integer transformJsRegion(Integer region) {
        Integer js_region;
        if (region == null || region < 100) {
            js_region = 0;
        } else {
            js_region = 1;
        }

        return js_region;
    }

    /**
     * description: 根据归属地对性别进行分类， 非本省 性别为 -99， 否则 1: 女, 0: 男，99: 未知
     * param: [sex]
     * return: java.lang.Integer
     **/
    public static final Short transformSexClass(Short sex, Integer region) {
        Short sexClass = sex;
        if (region == null || region < 100) {
            sexClass = -99;
        } else if(sex != 0 && sex != 1) {
            sexClass = 99;
        }
        return sexClass;
    }

    /**
     * description: 根据居住时段数据天数（uld）进行分类
     * param: [uld]
     * return: java.lang.Integer
     **/
    public static final Integer transformULDClass(Long uld) {
        Integer uld_class = 0;
        if(uld == 1) {
            uld_class = 3;
        } else if( uld > 1 && uld < 9) {
            uld_class = 2;
        } else if(uld >=9){
            uld_class = 1;
        }

        return uld_class;
    }


    /**
     * description: 对分析时间范围内的逗留时间进行分类,1：（0,5]；2：（5,10]（10,30]：3：（30,60]；4：（60,150]；5:（150,300]；6:（300,480]；7:（480,720]；8:（720,1440]
     * param: [stayTimeSeconds]
     * return: java.lang.Integer
     **/
    public static final Integer transformStayTimeClass(Double stayTimeMinutes) {
        Integer stayTimeClass;
        if (stayTimeMinutes > 0 && stayTimeMinutes <= 5) {
            stayTimeClass = 1;
        } else if (stayTimeMinutes > 5 && stayTimeMinutes <= 10) {
            stayTimeClass = 2;
        } else if (stayTimeMinutes > 10 && stayTimeMinutes <= 30) {
            stayTimeClass = 3;
        } else if (stayTimeMinutes > 30 && stayTimeMinutes <= 60) {
            stayTimeClass = 4;
        } else if (stayTimeMinutes > 60 && stayTimeMinutes <= 150) {
            stayTimeClass = 5;
        } else if (stayTimeMinutes > 150 && stayTimeMinutes <= 300) {
            stayTimeClass = 6;
        } else if (stayTimeMinutes > 300 && stayTimeMinutes <= 480) {
            stayTimeClass = 7;
        } else if (stayTimeMinutes > 480 && stayTimeMinutes <= 720) {
            stayTimeClass = 8;
        } else {
            stayTimeClass = 9;
        }
        return stayTimeClass;
    }
}
