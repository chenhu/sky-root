package com.sky.signal.stat.processor.signal;

import com.sky.signal.stat.config.ParamProperties;
import com.sky.signal.stat.util.FileUtil;
import org.apache.spark.sql.DataFrame;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.List;

/**
 * 加载有效信令数据
 */
@Service("signalLoader")
public class SignalLoader implements Serializable {
    @Autowired
    private transient ParamProperties params;
    public DataFrame load(List<String> validSignalFiles) {
        DataFrame validSignalDf = null;
        for(String validSignalFile: validSignalFiles) {
            if(validSignalDf == null) {
                validSignalDf = FileUtil.readFile(FileUtil.FileType.CSV, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA, validSignalFile)
                        .repartition(params.getPartitions());
            } else {
                validSignalDf = validSignalDf.unionAll(FileUtil.readFile(FileUtil.FileType.CSV, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA, validSignalFile)
                        .repartition(params.getPartitions()));
            }

        }
        return validSignalDf;
    }
}
