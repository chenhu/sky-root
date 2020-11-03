package com.sky.signal.pre.service.impl;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.processor.workLiveProcess.ExistsDayProcess;
import com.sky.signal.pre.service.ComputeService;
import com.sky.signal.pre.util.FileUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * 分批次预处理出现天数
 */
@Service
public class WorkLiveExistsDayService implements ComputeService {
    @Autowired
    private transient ExistsDayProcess existsDayProcess;
    @Autowired
    private transient ParamProperties params;

    @Override
    public void compute() {
        //删除临时数据
        this.clearTempExistsData();
        Map<Integer, List<String>> liveValidSignalFileMap = SelectSignalFilesByBatch.getBatchFiles(params.getValidSignalFilesForWorkLive(), params.getBatch());
        // 出现天数分批次预处理
        for( int batchId: liveValidSignalFileMap.keySet()) {
            List<String> validSignalFiles = liveValidSignalFileMap.get(batchId);
            existsDayProcess.process(SelectSignalFilesByBatch.getValidSignal(validSignalFiles), batchId);
        }
    }

    private void clearTempExistsData() {
        FileUtil.removeDfsDirectory(params.getExistsDaysSavePath());
    }

}