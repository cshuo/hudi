/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.table;

import org.apache.hudi.GlobalCluster;

import org.apache.flink.configuration.*;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.log4j.*;

import java.io.File;
import java.util.concurrent.CompletableFuture;

public class WriteDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(CoreOptions.FLINK_LOG_DIR, "file:///private/tmp/log");
        conf.set(RestOptions.PORT, 8088);
        conf.setString("rest.profiling.enabled", "true");
        MiniClusterConfiguration cfg =
            new MiniClusterConfiguration.Builder()
                .setNumTaskManagers(4)
                .setNumSlotsPerTaskManager(1)
                .setConfiguration(conf)
                .build();

        String logOutputDirectory = "file:///private/tmp/log/";

        File logOutputDir = new File(logOutputDirectory);
        if (!logOutputDir.exists()) {
            logOutputDir.mkdirs();
        }

        String logFilePath = logOutputDirectory + "flink.log";
        PatternLayout layout = new PatternLayout("%d{yyyy-MM-dd HH:mm:ss} %-5p %c{1}:%L - %m%n");
        RollingFileAppender appender = null;
        try {
            appender = new RollingFileAppender(layout, logFilePath);
            appender.setMaxFileSize("10MB");
            appender.setMaxBackupIndex(10);
        } catch (Exception e) {
            e.printStackTrace();
        }

        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);
        rootLogger.addAppender(appender);

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            GlobalCluster.setMiniCluster(miniCluster);
            Configuration envConf = new Configuration();
            envConf.set(StateBackendOptions.STATE_BACKEND, "filesystem");
            envConf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///private/tmp/cpt");
            envConf.set(CoreOptions.FLINK_LOG_DIR, "file:///private/tmp/log");
            envConf.setString("rest.profiling.enabled", "true");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                miniCluster.getRestAddress().get().getHost(),
                miniCluster.getRestAddress().get().getPort(),
                envConf);
            env.enableCheckpointing(120000, CheckpointingMode.EXACTLY_ONCE);
            env.setParallelism(4);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            String srcDdl = "CREATE TABLE item_uv_pv_1d_source (\n" +
                "    `item_id` BIGINT,\n" +
                "    `item_name` STRING,\n" +
                "    `item_click_uv_1d` BIGINT,\n" +
                "    `item_click_pv_1d` BIGINT,\n" +
                "    `item_like_uv_1d` BIGINT,\n" +
                "    `item_like_pv_1d` BIGINT,\n" +
                "    `item_cart_uv_1d` BIGINT,\n" +
                "    `item_cart_pv_1d` BIGINT,\n" +
                "    `item_share_uv_1d` BIGINT,\n" +
                "    `item_share_pv_1d` BIGINT\n" +
                ") WITH (\n" +
                "    'connector' = 'datagen',\n" +
                "    'number-of-rows' = '50000000',\n" +
                "    'rows-per-second' = '999999999',\n" +
                "    'fields.item_id.min' = '0',\n" +
                "    'fields.item_id.max' = '99999999',\n" +
                "    'fields.item_name.length' = '20',\n" +
                "    'fields.item_click_uv_1d.min' = '0',\n" +
                "    'fields.item_click_uv_1d.max' = '999999999',\n" +
                "    'fields.item_click_pv_1d.min' = '0',\n" +
                "    'fields.item_click_pv_1d.max' = '999999999',\n" +
                "    'fields.item_like_uv_1d.min' = '0',\n" +
                "    'fields.item_like_uv_1d.max' = '999999999',\n" +
                "    'fields.item_like_pv_1d.min' = '0',\n" +
                "    'fields.item_like_pv_1d.max' = '999999999',\n" +
                "    'fields.item_cart_uv_1d.min' = '0',\n" +
                "    'fields.item_cart_uv_1d.max' = '999999999',\n" +
                "    'fields.item_cart_pv_1d.min' = '0',\n" +
                "    'fields.item_cart_pv_1d.max' = '999999999',\n" +
                "    'fields.item_share_uv_1d.min' = '0',\n" +
                "    'fields.item_share_uv_1d.max' = '999999999',\n" +
                "    'fields.item_share_pv_1d.min' = '0',\n" +
                "    'fields.item_share_pv_1d.max' = '999999999'\n" +
                ");";
            tEnv.executeSql(srcDdl);

            tEnv.executeSql("CREATE VIEW item_uv_pv_1d AS\n" +
                "SELECT\n" +
                "    `item_id`,\n" +
                "    SUBSTR(`item_name`, 0, MOD(`item_id`, 32) + 64) AS `item_name`,\n" +
                "    `item_click_uv_1d`,\n" +
                "    `item_click_pv_1d`,\n" +
                "    `item_like_uv_1d`,\n" +
                "    `item_like_pv_1d`,\n" +
                "    `item_cart_uv_1d`,\n" +
                "    `item_cart_pv_1d`,\n" +
                "    `item_share_uv_1d`,\n" +
                "    `item_share_pv_1d`,\n" +
                "    NOW() AS `ts`\n" +
                "FROM item_uv_pv_1d_source;");

            String sinkDDL = "CREATE TABLE hudi_table(`item_id` BIGINT,\n" +
                "    `item_name` STRING,\n" +
                "    `item_click_uv_1d` BIGINT,\n" +
                "    `item_click_pv_1d` BIGINT,\n" +
                "    `item_like_uv_1d` BIGINT,\n" +
                "    `item_like_pv_1d` BIGINT,\n" +
                "    `item_cart_uv_1d` BIGINT,\n" +
                "    `item_cart_pv_1d` BIGINT,\n" +
                "    `item_share_uv_1d` BIGINT,\n" +
                "    `item_share_pv_1d` BIGINT,\n" +
                "    `ts` TIMESTAMP(3),\n" +
                "    PRIMARY KEY (`item_id`) NOT ENFORCED)\n" +
                "WITH ('connector' = 'hudi',\n" +
                "  'write.rowdata.mode.enabled' = 'true',\n" +
                "  'path' = 'file:///private/tmp/hudi_table',\n" +
                "  'table.type' = 'MERGE_ON_READ',\n" +
                "  'index.type' = 'BUCKET',\n" +
                "  'hoodie.bucket.index.num.buckets' = '2',\n" +
                "  'write.tasks' = '2',\n" +
                "  'hoodie.parquet.compression.codec' = 'snappy',\n" +
                "  'compaction.schedule.enabled' = 'false',\n" +
                "  'compaction.async.enabled' = 'false')";
            tEnv.executeSql(sinkDDL);
            String query0 = "insert into hudi_table SELECT * FROM item_uv_pv_1d";
            TableResult result = tEnv.executeSql(query0);

            result.await();
        }
    }
}
