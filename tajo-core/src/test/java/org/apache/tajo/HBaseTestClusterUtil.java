package org.apache.tajo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.master.ServerManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.zookeeper.MiniZooKeeperCluster;
import org.apache.tajo.util.Bytes;

import java.io.File;
import java.io.IOException;

import static org.apache.hadoop.hbase.HConstants.REPLICATION_ENABLE_KEY;

public class HBaseTestClusterUtil {
  private Configuration conf;
  private MiniHBaseCluster hbaseCluster;
  private MiniZooKeeperCluster zkCluster;
  private File testBaseDir;
  public HBaseTestClusterUtil(Configuration conf, File testBaseDir) {
    this.conf = conf;
    this.testBaseDir = testBaseDir;
  }
  /**
   * Returns the path to the default root dir the minicluster uses.
   * Note: this does not cause the root dir to be created.
   * @return Fully qualified path for the default hbase root dir
   * @throws java.io.IOException
   */
  public Path getDefaultRootDirPath() throws IOException {
    FileSystem fs = FileSystem.get(this.conf);
    return new Path(fs.makeQualified(fs.getHomeDirectory()),"hbase");
  }

  /**
   * Creates an hbase rootdir in user home directory.  Also creates hbase
   * version file.  Normally you won't make use of this method.  Root hbasedir
   * is created for you as part of mini cluster startup.  You'd only use this
   * method if you were doing manual operation.
   * @return Fully qualified path to hbase root dir
   * @throws IOException
   */
  public Path createRootDir() throws IOException {
    FileSystem fs = FileSystem.get(this.conf);
    Path hbaseRootdir = getDefaultRootDirPath();
    FSUtils.setRootDir(this.conf, hbaseRootdir);
    fs.mkdirs(hbaseRootdir);
    FSUtils.setVersion(fs, hbaseRootdir);
    return hbaseRootdir;
  }

  public void stopHBaseCluster() throws Exception {
    if (hbaseCluster != null) {
      hbaseCluster.shutdown();
    }

    if (zkCluster != null) {
      zkCluster.shutdown();
    }
  }

  public void startHBaseCluster() throws Exception {
    File zkDataPath = new File(testBaseDir, "zk");
    startMiniZKCluster(zkDataPath);

    System.setProperty("HBASE_ZNODE_FILE", testBaseDir + "/hbase_znode_file");
    if (conf.getInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, -1) == -1) {
      conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MINTOSTART, 1);
    }
    if (conf.getInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, -1) == -1) {
      conf.setInt(ServerManager.WAIT_ON_REGIONSERVERS_MAXTOSTART, 1);
    }
    conf.setBoolean(REPLICATION_ENABLE_KEY, false);
    createRootDir();

    Configuration c = HBaseConfiguration.create(this.conf);

    hbaseCluster = new MiniHBaseCluster(c, 1);

    // Don't leave here till we've done a successful scan of the hbase:meta
    HTable t = new HTable(c, TableName.META_TABLE_NAME);
    ResultScanner s = t.getScanner(new Scan());
    while (s.next() != null) {
      continue;
    }
    s.close();
    t.close();
  }

  /**
   * Start a mini ZK cluster. If the property "test.hbase.zookeeper.property.clientPort" is set
   *  the port mentionned is used as the default port for ZooKeeper.
   */
  private MiniZooKeeperCluster startMiniZKCluster(final File dir)
      throws Exception {
    if (this.zkCluster != null) {
      throw new IOException("Cluster already running at " + dir);
    }
    this.zkCluster = new MiniZooKeeperCluster(conf);
    final int defPort = this.conf.getInt("test.hbase.zookeeper.property.clientPort", 0);
    if (defPort > 0){
      // If there is a port in the config file, we use it.
      this.zkCluster.setDefaultClientPort(defPort);
    }
    int clientPort =  this.zkCluster.startup(dir, 1);
    this.conf.set(HConstants.ZOOKEEPER_CLIENT_PORT, Integer.toString(clientPort));
    return this.zkCluster;
  }

  public Configuration getConf() {
    return conf;
  }

  public MiniZooKeeperCluster getMiniZooKeeperCluster() {
    return zkCluster;
  }

  public MiniHBaseCluster getMiniHBaseCluster() {
    return hbaseCluster;
  }

  public HTableDescriptor getTableDescriptor(String tableName) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    return admin.getTableDescriptor(Bytes.toBytes(tableName));
  }

  public void createTable(HTableDescriptor hTableDesc) throws IOException {
    HBaseAdmin admin = new HBaseAdmin(conf);
    admin.createTable(hTableDesc);
  }
}
