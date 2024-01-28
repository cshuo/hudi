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

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.RollingFileAppender;

import java.io.File;
import java.util.Iterator;

/**
 * Created by cshuo on 2024/1/28
 */
public class ReadDemo {
    public static void main(String[] args) {
        Configuration envConf = new Configuration();
        envConf.set(StateBackendOptions.STATE_BACKEND, "filesystem");
        envConf.set(RestOptions.PORT, 8088);
        envConf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///private/tmp/cpt");
        envConf.set(CoreOptions.FLINK_LOG_DIR, "file:///private/tmp/log");

        MiniClusterConfiguration cfg =
            new MiniClusterConfiguration.Builder()
                .setNumTaskManagers(4)
                .setNumSlotsPerTaskManager(1)
                .setConfiguration(envConf)
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

            StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                "localhost",
                8088,
                envConf);
            env.enableCheckpointing(120000, CheckpointingMode.EXACTLY_ONCE);
            env.setParallelism(1);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

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
                "  'connector' = 'hudi',\n" +
                "  'read.start-commit' = 'earliest',\n" +
                "  'path' = 'file:///private/tmp/hudi_table',\n" +
                "  'table.type' = 'COPY_ON_WRITE'\n" +
                ");";
            tEnv.executeSql(sinkDDL);
            String query0 = "select count(distinct uuid) from hudi_table where city = '20240128'";
            Iterator<Row> res = tEnv.executeSql(query0).collect();
            System.out.println("### total rows: " + res.next());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}