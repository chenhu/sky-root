package com.sky.signal.stat.util;

/**
 * Created by ChenHu <chenhu1008@me.com> on 2020/3/25.
 */

import org.springframework.stereotype.Service;

/**
 * 淮安和深圳需求中用到的分类器
 * 将人口类别分为 0-常住人口 1-旅游商务人口 2-流动人口 3-其他
 */
@Service("huaianPersonClassification")
public  class HuaianPersonClassification implements PersonClassification {
    //逗留天数阀值，一般为3、7、9，要根据当前业务而定
    private static final Integer dayFlag = 9;
    //停留的时间阀值
    private static final Integer secondsFlag = 480*60;

    @Override
    public int classify( Long days, Double seconds) {
        Integer person_class;
        if (days >= dayFlag && seconds > secondsFlag) { //常住人口
            person_class = 0;
        } else if (days < dayFlag && days > 1 && seconds > secondsFlag) { // 旅游,商务人口
            person_class = 1;
        } else if (seconds <= secondsFlag) { // 流动人口
            person_class = 2;
        } else { // 其他人口
            person_class = 3;
        }
        return person_class;
    }

    /**
     * 根据居住时段数据天数（uld），日均逗留时长分类，对人口进行分类
     * @param uld
     * @param stay_time_class
     * @return
     */
    public int classify(Long uld, Integer stay_time_class) {
        Integer person_class;
        if (uld >= dayFlag && stay_time_class > 7) { //常住人口
            person_class = 0;
        } else if (uld < dayFlag && uld > 1 && stay_time_class > 7) { // 旅游,商务人口
            person_class = 1;
        } else if (stay_time_class <= 7) { // 流动人口
            person_class = 2;
        } else { // 其他人口
            person_class = 3;
        }
        return person_class;
    }
}
