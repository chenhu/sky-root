package com.sky.signal.pre.processor.workLiveProcess;

import java.io.Serializable;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/18 14:14
 * description: 数据归类服务
 */
public class TransformFunction implements Serializable {
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
     * description: 根据归属地对性别进行分类， 非本省 性别为 -1， 否则 1: 女, 0: 男
     * param: [sex]
     * return: java.lang.Integer
     **/
    public static final Short transformSexClass(Short sex, Integer region) {
        Short sexClass = sex;
        if (region == null || region < 100) {
            sexClass = -1;
        }
        return sexClass;
    }

    /**
     * description: 根据居住时段数据天数（uld），日均逗留时常，对人口进行分类
     * param: [uld, sumStayMinutes]
     * return: java.lang.Integer
     **/
    public static final Integer transformPersonClass(Long uld, Double sumStaySeconds) {
        Integer person_class;
        int tag = 3;
//        int tag = 7;

        // 常住人口，总逗留不小于7天，日均逗留时常大于48分钟
        if (uld >= tag && sumStaySeconds > 48 * 60) {
            person_class = 0;

        } else if (uld < tag && uld > 1 && sumStaySeconds > 48 * 60) { // 旅游,商务人口 总逗留时间1到7天，日均逗留时间大于48分钟
            person_class = 1;
        } else if (sumStaySeconds <= 48 * 60) { // 流动人口, 日均逗留时间不高于48分钟
            person_class = 2;
        } else { // 其他人口
            person_class = 3;
        }
        return person_class;
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
