package com.sky.signal.stat.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/6/6 21:34
 * description: 根据批次配置，获取每个批次文件
 */
public class FilesBatchUtils {
    public static Map<Integer, List<String>> getBatchFiles(List<String> fileList, int batches) {
        List<String> resultFileList = new ArrayList<>();
        Map<Integer, List<String>> batchMap = new HashMap<>();
        int batchNum = 1;
        for(int i=1; i <= fileList.size(); i++) {
            resultFileList.add(fileList.get(i - 1));
            if(i% batches == 0 || i == fileList.size()) {
                batchMap.put(batchNum, resultFileList);
                batchNum ++ ;
                resultFileList = new ArrayList<>() ;
                continue;
            }
        }
        return batchMap;
    }
}
