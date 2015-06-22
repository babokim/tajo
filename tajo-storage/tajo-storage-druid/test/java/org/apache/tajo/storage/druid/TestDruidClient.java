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

package org.apache.tajo.storage.druid;

import io.druid.query.Druids;
import io.druid.query.Druids.SelectorDimFilterBuilder;
import io.druid.query.filter.DimFilter;
import org.apache.tajo.storage.druid.client.DruidClient;
import org.apache.tajo.storage.druid.client.DruidServerInfo;
import org.apache.tajo.storage.druid.client.DruidUtil;

import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TestDruidClient {
  public static void main (String[] args) throws Exception {
    DruidClient client = new DruidClient("127.0.0.1:2181", "/druid");

    Collection<DruidServerInfo> coordinators = client.discoverDruidServers(DruidClient.DRUID_COORDINATOR);
    for (DruidServerInfo eachServer: coordinators) {
      System.out.println("coordinator>>>>" + eachServer);
    }
//
//    List<DruidServerInfo> brokers = client.discoverDruidServers(DruidClient.DRUID_BROKER);
//    for (DruidServerInfo eachServer: brokers) {
//      System.out.println("broker>>>>" + eachServer);
//    }
//
//    List<DruidServerInfo> realtimeServers = client.discoverDruidServers(DruidClient.DRUID_REALTIME_SERVER);
//    for (DruidServerInfo eachServer: realtimeServers) {
//      System.out.println("realtime>>>>" + eachServer);
//    }

    SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
    String intervalStart = DruidUtil.fromTimestamp(df.parse("2013-01-01").getTime(), false);
    String intervalEnd = DruidUtil.fromTimestamp(df.parse("2016-01-01").getTime(), false);
//    client.getDruidSegmentMetas(null, "test01", intervalStart, intervalEnd, false);

    DimFilter filter = Druids.newSelectorDimFilterBuilder()
        .dimension("col1").value("page-1").build();

    List<Map<String, Object>> rows = client.select(null, "test01", intervalStart, intervalEnd, filter);

    for(Map<String, Object> eachRow: rows) {
      printMap(eachRow);
    }
    System.out.println(">>>>>Total records: " + rows.size());

    client.close();
  }

  static void printMap(Map<String, Object> row) {
    String prefix = "";
    String line = "";
    for (Map.Entry<String, Object> eachColumn: row.entrySet()) {
      line += prefix + eachColumn.getKey() + "=" + eachColumn.getValue();
      prefix = ", ";
    }
    System.out.println(line);
  }
}
