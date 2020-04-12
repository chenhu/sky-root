package com.sky.signal.stat.util;

/**
 * Created by ChenHu <chenhu1008@me.com> on 2020/3/25.
 */

import org.springframework.stereotype.Service;

/**
 * 常熟需求中用到的人口分类器
 * 将人口类别分为 0-常住人口 1-流动人口 2-其他
 */
@Service("changshuPersonClassification")
public  class ChangshuPersonClassification implements PersonClassification {
    private static final Integer dayFlag = 20;// 30*2/3
    private static final Integer secondsFlag = 480*60;

    @Override
    public int classify( Long days, Double seconds) {
        Integer person_class;
        if (days >= dayFlag && seconds > secondsFlag) { //常住人口
            person_class = 0;
        } else if (days < dayFlag && days > 1 && seconds > secondsFlag) { // 流动人口
            person_class = 1;
        } else { // 访客人口
            person_class = 2;
        }
        return person_class;
    }
    /**
     * 根据居住时段数据天数（uld），日均逗留时长分类，对人口进行分类
     * @param uld
     * @param stay_time_class
     * @return
     */
    @Override
    public int classify(Long uld, Integer stay_time_class) {
        Integer person_class;
        if (uld >= dayFlag && stay_time_class > 7) { //常住人口
            person_class = 0;
        } else if (uld < dayFlag && uld > 1 && stay_time_class > 7) { // 流动人口
            person_class = 1;
        } else { // 访客人口
            person_class = 2;
        }
        return person_class;
    }
}
