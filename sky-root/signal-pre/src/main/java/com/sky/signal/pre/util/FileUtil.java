package com.sky.signal.pre.util;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.springframework.util.Assert;

/**
 * 文件服务
 */
public class FileUtil {
    public enum FileType {
        CSV, PARQUET
    }

    public static final String CSV_FORMAT = "com.databricks.spark.csv";

    private static final SQLContext sqlContext = ContextUtil.getApplicationContext().getBean(SQLContext.class);

    private FileUtil() {
    }

    /**
     * 写入文件
     * @param df
     * @param type
     * @param fileName
     */
    public static void saveFile(DataFrame df, FileType type, String fileName) {
        Assert.notNull(df, "DataFrame不可为空");
        Assert.hasLength(fileName, "文件名不可为空");

        switch (type) {
            case CSV:
                df.write().mode(SaveMode.Overwrite).format(CSV_FORMAT).option("header", "true").save(fileName);
                break;
            case PARQUET:
                df.write().mode(SaveMode.Overwrite).parquet(fileName);
                break;
        }
    }

    /**
     * 读取文件
     * @param type
     * @param schema
     * @param fileNames
     * @return
     */
    public static DataFrame readFile(FileType type, StructType schema, String... fileNames) {
        Assert.notNull(schema, "文件元数据不可为空");
        Assert.notEmpty(fileNames, "文件名不可为空");

        DataFrame result = null;
        for (String fileName : fileNames) {
            DataFrame df = null;
            switch (type) {
                case CSV:
                    df = sqlContext.read().format(CSV_FORMAT).schema(schema).option("header", "true").option("nullValue", "null").option("treatEmptyValuesAsNulls,","true").load(fileName);
                    break;
                case PARQUET:
                    df = sqlContext.read().parquet(fileName);
                    break;
            }
            if (result == null) {
                result = df;
            } else {
                result = result.unionAll(df);
            }
        }
        return result;
    }
}
