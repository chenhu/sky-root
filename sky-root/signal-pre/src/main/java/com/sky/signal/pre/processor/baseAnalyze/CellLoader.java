package com.sky.signal.pre.processor.baseAnalyze;

import com.sky.signal.pre.config.ParamProperties;
import com.sky.signal.pre.util.FileUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/6 16:21
 * description: 加载已经处理好的基站信息数据，并广播
 */
@Service("cellLoader")
public class CellLoader implements Serializable {
    @Autowired
    private  JavaSparkContext sparkContext;
    @Autowired
    private transient ParamProperties params;
    public Broadcast<Map<String, Row>> load() {
        // 加载现有的基站数据,这个数据是由 cellProcess.process() 生成的
        Row[] cellRows= FileUtil.readFile(FileUtil.FileType.CSV, CellSchemaProvider.CELL_SCHEMA,params.getSavePath() + "cell").collect();
        Map<String, Row> cellMap = new HashMap<>(cellRows.length);
        for (Row row : cellRows) {
            Integer tac = row.getInt(1);
            Long cell = row.getLong(2);
            cellMap.put(tac.toString() + '|' + cell.toString(), row);
        }
        final Broadcast<Map<String, Row>> cellVar = sparkContext.broadcast(cellMap);
        return cellVar;
    }
}
