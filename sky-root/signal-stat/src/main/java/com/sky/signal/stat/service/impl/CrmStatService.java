package com.sky.signal.stat.service.impl;

import com.google.common.collect.Lists;
import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.processor.workLive.LiveWorkSchemaProvider;
import com.sky.signal.stat.service.ComputeService;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.count;

/**
 * crm情况统计
 */
@Service("crmStatService")
public class CrmStatService implements ComputeService {
    private static final Logger logger = LoggerFactory.getLogger(CrmStatService.class);
    @Autowired
    private transient ParamProperties params;
    public static final StructType CRM_SCHEMA = DataTypes.createStructType(Lists.newArrayList(
            DataTypes.createStructField("msisdn", DataTypes.StringType, false),
            DataTypes.createStructField("sex", DataTypes.ShortType, false),
            DataTypes.createStructField("age", DataTypes.ShortType, false),
            DataTypes.createStructField("id", DataTypes.IntegerType, false)));
    @Override
    public void compute() {
        DataFrame crm = FileUtil.readFile(FileUtil.FileType.CSV, CRM_SCHEMA, params.getBaseFile() + "crm");
        crm = crm.groupBy("sex", "age").agg(count("*").as("num"));
        FileUtil.saveFile(crm.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/crm");
        DataFrame workliveSource = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.WORK_LIVE_SCHEMA, params.getWorkLiveFile());
        workliveSource = workliveSource.groupBy("region","sex","age").agg(count("*").as("num"));
        FileUtil.saveFile(workliveSource.repartition(params.getStatpatitions()), FileUtil.FileType.CSV, params.getSavePath() + "stat/worklivecheck");
    }
}
