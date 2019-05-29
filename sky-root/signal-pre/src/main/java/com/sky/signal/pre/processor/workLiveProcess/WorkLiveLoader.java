package com.sky.signal.pre.processor.workLiveProcess;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

/**
 * 加载职住分析的数据
 */
@Service
public class WorkLiveLoader implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(WorkLiveLoader.class);
    @Autowired
    private transient ParamProperties params;
    public DataFrame load(String workLiveFile) {
        DataFrame df = FileUtil.readFile(FileUtil.FileType.CSV, LiveWorkSchemaProvider.WORK_LIVE_SCHEMA, workLiveFile)
                .repartition(params.getPartitions());
        return df;
    }
}
