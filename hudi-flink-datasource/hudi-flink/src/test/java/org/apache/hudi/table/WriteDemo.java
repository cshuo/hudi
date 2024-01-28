package org.apache.hudi.table;

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

/**
 * Created by cshuo on 2024/1/28
 */
public class WriteDemo {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set(CoreOptions.FLINK_LOG_DIR, "file:///private/tmp/log");
        conf.set(RestOptions.PORT, 8088);
        MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(4)
                        .setNumSlotsPerTaskManager(1)
                        .setConfiguration(conf)
                        .build();

        String logOutputDirectory = "file:///private/tmp/log/";

        // 创建日志输出目录（如果不存在）
        File logOutputDir = new File(logOutputDirectory);
        if (!logOutputDir.exists()) {
            logOutputDir.mkdirs();
        }

        // 配置日志记录器
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

        // 设置根日志级别和输出目标
        Logger rootLogger = Logger.getRootLogger();
        rootLogger.setLevel(Level.INFO);
        rootLogger.addAppender(appender);

        try (final MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            Configuration envConf = new Configuration();
            envConf.set(StateBackendOptions.STATE_BACKEND, "filesystem");
            envConf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///private/tmp/cpt");
            envConf.set(CoreOptions.FLINK_LOG_DIR, "file:///private/tmp/log");
            StreamExecutionEnvironment env = StreamExecutionEnvironment.createRemoteEnvironment(
                    miniCluster.getRestAddress().get().getHost(),
                    miniCluster.getRestAddress().get().getPort(),
                    envConf);
            env.enableCheckpointing(120000, CheckpointingMode.EXACTLY_ONCE);
            env.setParallelism(1);
            EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
            StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

            String srcDdl = "create temporary table src (a int, b bigint, c string) with ('connector' = 'datagen', 'rows-per-second' = '100')";
            tEnv.executeSql(srcDdl);
            String dummySink = "create temporary table dummy_sink(a int, b bigint, c string) with ('connector' = 'blackhole')";
            tEnv.executeSql(dummySink);
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
                    "  'path' = 'file:///private/tmp/hudi_table',\n" +
                    "  'table.type' = 'MERGE_ON_READ'\n" +
                    ");";
            tEnv.executeSql(sinkDDL);
            String query0 = "insert into hudi_table partition (city='20240128') select UNIX_TIMESTAMP(), c, c, c, cast(a as double) / b from src";
            String query1 = "insert into dummy_sink select a, max(b), min(c) from src group by a";
            TableResult result = tEnv.executeSql(query0);
            result.await();
        }


    }
}
