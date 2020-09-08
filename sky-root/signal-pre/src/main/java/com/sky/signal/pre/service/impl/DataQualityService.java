package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.dataquality.DataQualityProcessor;
import com.sky.signal.pre.processor.dataquality.DataQualitySchemaProvider;
import com.sky.signal.pre.service.ComputeService;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Tuple2;

import java.util.Map;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/5/13 17:22
 * description: 原始数据质量验证服务
 */
@Service("dataQualityService")
public class DataQualityService implements ComputeService {
    @Autowired
    private transient ParamProperties params;

    @Autowired
    private transient DataQualityProcessor dataQualityProcessor;
    @Override
    public void compute() {
        Map<String, Tuple2<String, String>> signalMap = params.getSignalFilePathTuple2();
        for(String date: signalMap.keySet()) {
            String traceFilePath = signalMap.get(date)._2;
            String validSignalFilePath = signalMap.get(date)._1;
            dataQualityProcessor.process(date,traceFilePath, validSignalFilePath);
        }
        DataFrame dataQualityAllStat = FileUtil.readFile(FileUtil.FileType.CSV, DataQualitySchemaProvider.SIGNAL_SCHEMA_BASE, params.getDataQualitySavePath("*")).orderBy("date");
        FileUtil.saveFile(dataQualityAllStat.repartition(1), FileUtil.FileType.CSV, params.getDataQualityAllSavePath());

    }
}
