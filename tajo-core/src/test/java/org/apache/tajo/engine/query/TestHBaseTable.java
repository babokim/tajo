/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.query;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.net.InetAddress;

import static org.junit.Assert.*;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@Category(IntegrationTest.class)
public class TestHBaseTable extends QueryTestCaseBase {

  @Test
  public void testVerifyCreateHBaseTableRequiredMeta() throws Exception {
    try {
      executeString("CREATE TABLE hbase_mapped_table1 (col1 text, col2 text) " +
          "USING hbase").close();

      fail("hbase table must have 'table' meta");
    } catch (Exception e) {
      assertTrue(e.getMessage().indexOf("HBase mapped table") >= 0);
    }

    try {
      executeString("CREATE TABLE hbase_mapped_table1 (col1 text, col2 text) " +
          "USING hbase " +
          "WITH ('table'='hbase_table')").close();

      fail("hbase table must have 'columns' meta");
    } catch (Exception e) {
      assertTrue(e.getMessage().indexOf("HBase mapped table") >= 0);
    }

    try {
      executeString("CREATE TABLE hbase_mapped_table1 (col1 text, col2 text) " +
          "USING hbase " +
          "WITH ('table'='hbase_table', 'columns'='col1:,col2:')").close();

      fail("hbase table must have 'hbase.zookeeper.quorum' meta");
    } catch (Exception e) {
      assertTrue(e.getMessage().indexOf("HBase mapped table") >= 0);
    }
  }

  @Test
  public void testCreateHBaseTable() throws Exception {
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

    executeString("CREATE TABLE hbase_mapped_table1 (col1 text, col2 text, col3 text, col4 text) " +
        "USING hbase WITH ('table'='hbase_table', 'columns'=':key,col2:a,col3:,col2:b', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("hbase_mapped_table1");

    HTableDescriptor hTableDesc = testingCluster.getHBaseUtil().getTableDescriptor("hbase_table");
    assertNotNull(hTableDesc);
    assertEquals("hbase_table", hTableDesc.getNameAsString());

    HColumnDescriptor[] hColumns = hTableDesc.getColumnFamilies();
    // col1 is mapped to rowkey
    assertEquals(2, hColumns.length);
    assertEquals("col2", hColumns[0].getNameAsString());
    assertEquals("col3", hColumns[1].getNameAsString());

    executeString("DROP TABLE hbase_mapped_table1 PURGE");
  }

  @Test
  public void testCreateNotExistsExternalHBaseTable() throws Exception {
    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

    try {
      executeString("CREATE EXTERNAL TABLE external_hbase_mapped_table1 (col1 text, col2 text, col3 text, col4 text) " +
          "USING hbase WITH ('table'='external_hbase_table', 'columns'=':key,col2:a,col3:,col2:b', " +
          "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
          "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();
      fail("External table should be a existed table.");
    } catch (Exception e) {
      assertTrue(e.getMessage().indexOf("External table should be a existed table.") >= 0);
    }
  }

  @Test
  public void testCreateExternalHBaseTable() throws Exception {
    HTableDescriptor hTableDesc = new HTableDescriptor(TableName.valueOf("external_hbase_table"));
    hTableDesc.addFamily(new HColumnDescriptor("col1"));
    hTableDesc.addFamily(new HColumnDescriptor("col2"));
    hTableDesc.addFamily(new HColumnDescriptor("col3"));
    testingCluster.getHBaseUtil().createTable(hTableDesc);

    String hostName = InetAddress.getLocalHost().getHostName();
    String zkPort = testingCluster.getHBaseUtil().getConf().get(HConstants.ZOOKEEPER_CLIENT_PORT);
    assertNotNull(zkPort);

    executeString("CREATE EXTERNAL TABLE external_hbase_mapped_table (rk text, col1 text, col2 text, col3 text) " +
        "USING hbase WITH ('table'='external_hbase_table', 'columns'=':key,col1:a,col2:,col3:b', " +
        "'" + HConstants.ZOOKEEPER_QUORUM + "'='" + hostName + "'," +
        "'" + HConstants.ZOOKEEPER_CLIENT_PORT + "'='" + zkPort + "')").close();

    assertTableExists("external_hbase_mapped_table");

    executeString("DROP TABLE external_hbase_mapped_table PURGE");
  }
}
