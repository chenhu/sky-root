package com.sky.signal.pre.processor.workLiveProcess;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.config.PathConfig;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;

/**
 * 加载职住分析的数据
 */
@Service("workLiveLoader")
public class WorkLiveLoader implements Serializable {
    @Autowired
    private transient ParamProperties params;

    public DataFrame load() {
        DataFrame df = FileUtil.readFile(FileUtil.FileType.CSV,
                LiveWorkSchemaProvider.WORK_LIVE_SCHEMA, params
                        .getWorkliveSavePath() + PathConfig.WORKLIVE_PATH)
                .repartition(params.getPartitions());
        return df;
    }
}
