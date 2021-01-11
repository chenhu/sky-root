/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.sky.signal.stat.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;


import java.io.*;

/**
 * json文件转换为csv格式
 *
 * @author chenhu
 * 2021/1/11 11:31
 **/
public class Json2Csv {
    public static void main(String... args) {
        String jsonFilePath = "/Users/chenhu/Documents/wildSky/交换文件夹.nosync/day-base-1000515/day-base-1000515";
        String csvFilePath = jsonFilePath.concat(".csv");
        File writeName = new File(csvFilePath);
        try (FileReader reader = new FileReader(jsonFilePath);
             FileWriter writer = new FileWriter(writeName);
             BufferedWriter out = new BufferedWriter(writer);
             BufferedReader br = new BufferedReader(reader)
        ) {
            writeName.createNewFile();
            String line;
            out.write("date,geohash\r\n");

            while ((line = br.readLine()) != null) {
                JSONObject jsonObject = JSON.parseObject(line);

                out.write(jsonObject.getString("date").concat(",").concat(jsonObject.getString("geohash")).concat("\r\n"));

                out.flush(); // 把缓存区内容压入文件
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
