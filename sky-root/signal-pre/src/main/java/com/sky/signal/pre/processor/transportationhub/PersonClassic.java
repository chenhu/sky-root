package com.sky.signal.pre.processor.transportationhub;

/**
 * Created by Chenhu on 2020/5/30.
 * 枢纽站人口分类枚举
 */
public enum PersonClassic {
    Leave("枢纽站出发人口", 0), Leave1("枢纽站出发人口（当天往返，但非当前枢纽返回）", 1),
    Arrive("到达人口", 2), TransportationHubPassBy("枢纽站过境人口", 3),
    CityPassBy("城市过境人口", 4), NotTransportationHubLeave("非高铁出行对外交通人口", 5);

    private String name;
    private Integer index;

    PersonClassic(String name, Integer index) {
        this.name = name;
        this.index = index;
    }

    public static String getName(Integer index) {
        for (PersonClassic c : PersonClassic.values()) {
            if (c.getIndex() == index) {
                return c.name;
            }
        }
        return null;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Integer getIndex() {
        return index;
    }

    public void setIndex(Integer index) {
        this.index = index;
    }

    public static void main(String... args) {
        System.out.println(PersonClassic.TransportationHubPassBy.getIndex());
    }


}
