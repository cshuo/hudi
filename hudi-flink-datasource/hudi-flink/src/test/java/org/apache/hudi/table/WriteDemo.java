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
            env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);
            env.setParallelism(3);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            String srcDdl = "create temporary table src (a int, b bigint, c string) with " +
                "('connector' = 'datagen', 'rows-per-second' = '1000', 'fields.a.kind' = 'sequence', 'fields.a.start' = '1', 'fields.a.end' = '200000')";
            tEnv.executeSql(srcDdl);

            String sinkDDL = "CREATE TABLE hudi_table(\n" +
                "    ts BIGINT,\n" +
                "    uuid VARCHAR(40) PRIMARY KEY NOT ENFORCED,\n" +
                "    rider VARCHAR(20),\n" +
                "    driver VARCHAR(20),\n" +
                "    fare DOUBLE,\n" +
                "    city VARCHAR(20)\n" +
                ")\n" +
                "PARTITIONED BY (`city`)\n" +
                "WITH (\n" +
                "'connector' = 'hudi', \n" +
                "  'path' = 'file:///private/tmp/hudi_table',\n" +
                "  'table.type' = 'COPY_ON_WRITE', \n" +
                "  'hoodie.clean.async' = 'true', \n" +
                "  'hoodie.cleaner.policy' = 'KEEP_LATEST_COMMITS', \n" +
                "  'hoodie.clean.automatic' = 'true', \n" +
                "  'hoodie.clean.max.commits' = '8', \n" +
                "  'hoodie.clean.trigger.strategy' = 'NUM_COMMITS', \n" +
                "  'hoodie.cleaner.parallelism' = '100', \n" +
                "  'hoodie.cleaner.commits.retained' = '6', \n" +
                "  'hoodie.index.type' = 'BUCKET',\n" +
                "  'hoodie.index.bucket.engine' = 'SIMPLE', \n" +
                "  'hoodie.bucket.index.num.buckets' = '16', \n" +
                "  'hoodie.parquet.small.file.limit' = '104857600', \n" +
                "  'hoodie.parquet.compression.codec' = 'snappy', \n" +
                "  'hoodie.schema.on.read.enable' = 'true', \n" +
                "  'hoodie.archive.automatic' = 'true', \n" +
                "  'hoodie.keep.max.commits' = '45', \n" +
                "  'hoodie.keep.min.commits' = '30')";
            tEnv.executeSql(sinkDDL);
            String query0 = "insert into hudi_table partition (city='20240128') select UNIX_TIMESTAMP(), concat('k-', cast(a as string)), c, c, cast(a as double) / b from src";
            TableResult result = tEnv.executeSql(query0);

            result.await();
        }
    }
}
