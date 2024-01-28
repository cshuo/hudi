package org.apache.hudi;

import org.apache.flink.runtime.minicluster.MiniCluster;

public class GlobalCluster {
  public static MiniCluster miniCluster;
  public static boolean stopped = false;
  public static void setMiniCluster(MiniCluster cluster) {
    miniCluster = cluster;
  }
  public static void stopTm(int index) {
    if (stopped) {
      System.out.println("##### already stopped #####");
      return;
    }
    miniCluster.terminateTaskManager(index);
    System.out.println("##### stopped successfully #####");
    stopped = true;
  }
}
