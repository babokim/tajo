/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.query;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.storage.druid.DruidStorageConstants;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.net.InetAddress;
import java.sql.ResultSet;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestDruidQuery extends QueryTestCaseBase {
  private static final Log LOG = LogFactory.getLog(TestDruidQuery.class);

  private static String hostName;
  private static String zkPort;

  @BeforeClass
  public static void beforeClass() {
    try {
      //TODO start druid cluster
      hostName = InetAddress.getLocalHost().getHostName();
      zkPort = "2181";
      assertNotNull(hostName);
      assertNotNull(zkPort);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @AfterClass
  public static void afterClass() {
    try {
      //TODO stop druid cluster
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Test
  public void testCreateDruidMappedTable() throws Exception {
    try {
      executeString("CREATE TABLE druid_mapped_table_error (col1 text, col2 float4, col3 float4, col4 int8) " +
          "USING druid WITH ('dataSource'='test01', 'columns'='col1,col2,col3,num_rows', " +
          "'" + DruidStorageConstants.META_ZOOKEEPER_QUORUM + "'='" + hostName + ":2181'," +
          "'" + DruidStorageConstants.META_ZOOKEEPER_ROOT_PATH + "'='/druid')").close();
      fail("Druid mapped table can't be made without 'timestamp' column mapping.");
    } catch (Exception e) {
      //success
    }
    assertTableNotExists("druid_mapped_table_error");

    executeString("CREATE TABLE druid_mapped_table1 (col1 text, col2 float4, col3 float4, col4 int8, time_col int8) " +
        "USING druid WITH ('dataSource'='test01', 'columns'='col1,col2,col3,num_rows,timestamp', " +
        "'" + DruidStorageConstants.META_ZOOKEEPER_QUORUM + "'='" + hostName + ":2181'," +
        "'" + DruidStorageConstants.META_ZOOKEEPER_ROOT_PATH + "'='/druid')").close();
    assertTableExists("druid_mapped_table1");

    //simple query
    ResultSet rs = executeString("select col2, col1, time_col from druid_mapped_table1");

    String result = "col2,col1,time_col\n" +
        "-------------------------------\n" +
        "0.0,page-0,1432611599000\n" +
        "1.0,page-1,1432611599000\n" +
        "8.0,page-2,1432611599000\n" +
        "3.0,page-3,1432611599000\n" +
        "12.0,page-4,1432611599000\n" +
        "3.0,page-3,1432611737000\n" +
        "0.0,page-0,1432612499000\n" +
        "4.0,page-1,1432612499000\n" +
        "2.0,page-2,1432612499000\n" +
        "6.0,page-3,1432612499000\n" +
        "0.0,page-0,1432612589000\n" +
        "0.0,page-0,1432613099000\n" +
        "2.0,page-1,1432613099000\n" +
        "9.0,page-3,1432613099000\n" +
        "16.0,page-4,1432613099000\n" +
        "0.0,page-0,1432613388000\n" +
        "2.0,page-1,1432613388000\n" +
        "3.0,page-3,1432613388000\n" +
        "12.0,page-4,1432613388000\n";

    assertEquals(result, resultSetToString(rs));
    rs.close();

    //filtered query
    rs = executeString("select col2, col1, time_col from druid_mapped_table1 where col1 = 'page-1'");

    result = "col2,col1,time_col\n" +
        "-------------------------------\n" +
        "1.0,page-1,1432611599000\n" +
        "4.0,page-1,1432612499000\n" +
        "2.0,page-1,1432613099000\n" +
        "2.0,page-1,1432613388000\n";

    assertEquals(result, resultSetToString(rs));

    rs.close();
  }
}
