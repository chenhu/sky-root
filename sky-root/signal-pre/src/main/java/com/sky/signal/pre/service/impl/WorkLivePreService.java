package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.workLiveProcess.LiveProcess;
import com.sky.signal.pre.processor.workLiveProcess.WorkProcess;
import com.sky.signal.pre.service.ComputeService;
import com.sky.signal.pre.util.FileUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 职住分析分批次预处理
 */
@Service("workLivePreService")
public class WorkLivePreService implements ComputeService {
    @Autowired
    private transient WorkProcess workProcess;
    @Autowired
    private transient LiveProcess liveProcess;
    @Autowired
    private transient ParamProperties params;

    @Override
    public void compute() {
        //清理临时数据
        this.clearTempWorkLiveData();
        Map<Integer, List<String>> workLiveValidSignalFileMap = SelectSignalFilesByBatch.getBatchFiles(params.getValidSignalFilesForWorkLive(), params.getBatch());
        // 居住地分批次预处理
        for( int batchId: workLiveValidSignalFileMap.keySet()) {
            List<String> validSignalFiles = workLiveValidSignalFileMap.get(batchId);
            liveProcess.process(SelectSignalFilesByBatch.getValidSignal(validSignalFiles), batchId);
        }
        // 工作地分批次预处理
        for( int batchId: workLiveValidSignalFileMap.keySet()) {
            List<String> validSignalFiles = workLiveValidSignalFileMap.get(batchId);
            workProcess.process(SelectSignalFilesByBatch.getValidSignal(validSignalFiles), batchId);
        }
    }

    private void clearTempWorkLiveData() {
        FileUtil.removeDfsDirectory(params.getLiveSavePath());
        FileUtil.removeDfsDirectory(params.getWorkSavePath());
    }


}