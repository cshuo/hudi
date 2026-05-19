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

package org.apache.hudi.streamer;

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.sink.utils.Pipelines;
import org.apache.hudi.source.RollbackFileMonitoringFunction;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A test pipeline that monitors Hudi rollback instants and sends findings to a dummy sink.
 */
public class RollbackFileMonitorPipeline {

  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, null, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    List<String> tablePaths = cfg.getTablePaths();
    Configuration conf = cfg.toFlinkConfig();
    env.getConfig().setGlobalJobParameters(conf);

    DataStream<Object> source = env
        .addSource(
            new RollbackFileMonitoringFunction(conf, tablePaths, cfg.checkIntervalSeconds),
            TypeInformation.of(Object.class))
        .name("rollback_file_monitor_source")
        .uid("uid_rollback_file_monitor_source")
        .setParallelism(1);

    Pipelines.dummySink(source);
    env.execute(cfg.jobName);
  }

  /**
   * Configurations for the rollback file monitor pipeline.
   */
  public static class Config {
    @Parameter(names = {"--table-paths"}, description = "Comma-separated Hudi table base paths to monitor.", required = true)
    public String tablePaths;

    @Parameter(names = {"--check-interval-seconds"}, description = "Seconds between two table scans.")
    public Long checkIntervalSeconds = 180L;

    @Parameter(names = {"--job-name"}, description = "Flink job name.")
    public String jobName = "hudi-rollback-file-monitor";

    @Parameter(names = {"--hoodie-conf"}, description = "Extra Flink/Hadoop config, for example hadoop.fs.defaultFS=hdfs://nn.")
    public List<String> configs = new ArrayList<>();

    @Parameter(names = {"--help", "-h"}, help = true)
    public Boolean help = false;

    public List<String> getTablePaths() {
      ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(tablePaths), "Option --table-paths must not be empty.");
      List<String> paths = Arrays.stream(tablePaths.split(","))
          .map(String::trim)
          .filter(path -> !path.isEmpty())
          .collect(Collectors.toList());
      ValidationUtils.checkArgument(!paths.isEmpty(), "Option --table-paths must contain at least one table path.");
      return paths;
    }

    public Configuration toFlinkConfig() {
      ValidationUtils.checkArgument(
          checkIntervalSeconds != null && checkIntervalSeconds > 0,
          "Option --check-interval-seconds must be positive.");
      Configuration conf = new Configuration();
      configs.forEach(config -> {
        String[] kv = config.split("=", 2);
        ValidationUtils.checkArgument(kv.length == 2, "Invalid --hoodie-conf: " + config);
        conf.setString(kv[0], kv[1]);
      });
      return conf;
    }
  }
}
