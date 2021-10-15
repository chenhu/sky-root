/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.sky.signal.pre.processor.district;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.baseAnalyze.CellLoader;
import com.sky.signal.pre.processor.signalProcess.SignalLoader;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.Map;

import static org.apache.spark.sql.functions.count;

/**
 * 统计手机号码数量
 * 按照 [指定日期]+地市+区县 进行统计手机号数量，也就是MSISDN数量
 * 特别说明：
 * 1. 被统计的号码，不需要满足 有跨区县出行动作 这个条件
 * 2. 分析后生成的结果，因为需要能直接查看，所以应该为文本格式
 * 3. 本来统计分析不应该处于这个位置，但是因为统计的内容比较原始，所以暂时放到这个地方，更方便些
 * @author chenhu
 * 2021/10/14 10:46
 **/
@Component
public class DistrictMsisdnStatProcessor {
    @Autowired
    private transient CellLoader cellLoader;
    @Autowired
    private transient ParamProperties params;
    @Autowired
    private transient SignalLoader signalLoader;
    @Autowired
    private transient SQLContext sqlContext;
    @Autowired
    private transient JavaSparkContext sparkContext;
    public void process() {
        final Broadcast<Map<String, Row>> cellVar = cellLoader.load(params.getCellSavePath());
        for (String date : params.getStrDay().split(",")) {
            //加载要处理的地市的信令
            String tracePath = params.getTraceFiles(Integer.valueOf(date));
            //合并基站信息到信令数据中
            DataFrame sourceDf = sqlContext.read().parquet(tracePath).repartition(params.getPartitions());
            sourceDf = signalLoader.cell(cellVar).mergeCell(sourceDf)
                    .groupBy("date","city_code","district_code")
                    .agg(count("msisdn").as("num")).orderBy("date","city_code","district_code");
            FileUtil.saveFile(sourceDf, FileUtil.FileType.CSV, params.getDistrictMsisdnCountStatPath());
        }
    }
}
