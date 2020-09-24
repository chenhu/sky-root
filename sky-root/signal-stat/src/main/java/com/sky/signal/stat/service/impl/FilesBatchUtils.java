package com.sky.signal.stat.service.impl;

import java.util.*;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/6/6 21:34
 * description: 根据批次配置，获取每个批次文件
 */
public class FilesBatchUtils {
    public static Map<Integer, List<String>> getBatchFiles(List<String> fileList, int batches, int crashBatch) {
        crashBatch = Math.max(crashBatch, 1);
        List<String> leftFileList = new ArrayList<>();
        if(crashBatch > 1) {
            for (int i=0;i < fileList.size(); i++) {
                if(i >= (crashBatch-1) * batches ) {
                    leftFileList.add(fileList.get(i));
                }
            }
        }else {
            leftFileList.addAll(fileList);
        }
        List<String> resultFileList = new ArrayList<>();
        Map<Integer, List<String>> batchMap = new HashMap<>();

        for (int i = 1 ; i <= leftFileList.size(); i++) {
            resultFileList.add(leftFileList.get(i-1));
            if ( i % batches == 0 || i == leftFileList.size() ) {
                batchMap.put(crashBatch, resultFileList);
                crashBatch++;
                resultFileList = new ArrayList<>();
                continue;
            }
        }
        return batchMap;
    }
}

