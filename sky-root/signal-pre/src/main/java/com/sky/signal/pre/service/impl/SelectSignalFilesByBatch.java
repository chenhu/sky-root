package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.processor.signalProcess.SignalSchemaProvider;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.sql.DataFrame;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/6/6 21:34
 * description: 根据批次配置，获取每个批次文件
 */
public class SelectSignalFilesByBatch {
    public static Map<Integer, List<String>> getBatchFiles(List<String> fileList, int workLiveBatchs) {
        List<String> batchValidSignalList = new ArrayList<>();
        Map<Integer, List<String>> batchMap = new HashMap<>();
        int batchNum = 1;
        for(int i=1; i <= fileList.size(); i++) {
            batchValidSignalList.add(fileList.get(i - 1));
            if(i% workLiveBatchs == 0 || i == fileList.size()) {
                batchMap.put(batchNum, batchValidSignalList);
                batchNum ++ ;
                batchValidSignalList = new ArrayList<>() ;
                continue;
            }
        }
        return batchMap;
    }

    public static DataFrame getValidSignal(List<String> validSignalFiles) {
        DataFrame validSignalDF = null;
        for (String ValidSignalFile : validSignalFiles) {
            DataFrame validDF = FileUtil.readFile(FileUtil.FileType.CSV, SignalSchemaProvider.SIGNAL_SCHEMA_NO_AREA, ValidSignalFile);
            if (validSignalDF == null) {
                validSignalDF = validDF;
            } else {
                validSignalDF = validSignalDF.unionAll(validDF);
            }
        }

        return validSignalDF;
    }
}
