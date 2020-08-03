package com.sky.signal.population.processor;

import com.sky.signal.population.config.ParamProperties;
import com.sky.signal.population.util.FileUtil;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.spark.sql.functions.col;

/**
 * author: ChenHu <chenhu1008@me.com>
 * date: 2019/4/6 16:21
 * description: 加载已经处理好的基站信息数据，并广播
 */
@Service("cellLoader")
public class CellLoader implements Serializable {
    @Autowired
    private JavaSparkContext sparkContext;
    @Autowired
    private transient ParamProperties params;

    /**
     * 加载预处理生成的基站数据
     * @return 广播基站
     */
    public Broadcast<Map<String, Row>> loadCell() {
        DataFrame cellDf = FileUtil.readFile(FileUtil.FileType.CSV,
                CellSchemaProvider.CELL_SCHEMA, params.getValidProvinceCellPath());
        Row[] cellRows = cellDf.collect();
        Map<String, Row> cellMap = new HashMap<>(cellRows.length);
        for (Row row : cellRows) {
            Integer tac = Integer.valueOf(row.getAs("tac").toString()) ;
            Integer cell = Integer.valueOf(row.getAs("cell").toString()) ;
            cellMap.put(tac.toString() + '|' + cell.toString(), row);
        }
        final Broadcast<Map<String, Row>> cellVar = sparkContext.broadcast
                (cellMap);
        return cellVar;
    }

}
