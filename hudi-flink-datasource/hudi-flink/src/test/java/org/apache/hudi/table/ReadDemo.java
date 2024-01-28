package org.apache.hudi.table;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Iterator;

/**
 * Created by cshuo on 2024/1/28
 */
public class ReadDemo {
    public static void main(String[] args) {
        Configuration envConf = new Configuration();
        envConf.set(StateBackendOptions.STATE_BACKEND, "filesystem");
        envConf.set(CheckpointingOptions.CHECKPOINTS_DIRECTORY, "file:///private/tmp/cpt");
        envConf.set(CoreOptions.FLINK_LOG_DIR, "file:///private/tmp/log");
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
                "  'path' = 'file:///private/tmp/hudi_table',\n" +
                "  'table.type' = 'MERGE_ON_READ'\n" +
                ");";
        tEnv.executeSql(sinkDDL);
        String query0 = "select count(*) from hudi_table where city = '20240128'";
        Iterator<Row> res = tEnv.executeSql(query0).collect();
        System.out.println("############## data ################");
        while (res.hasNext()) {
            System.out.println(res.next());
        }
    }
}
