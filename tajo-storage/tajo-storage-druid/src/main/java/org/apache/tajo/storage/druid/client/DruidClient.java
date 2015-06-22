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

package org.apache.tajo.storage.druid.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import io.druid.granularity.QueryGranularity;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.Druids;
import io.druid.query.Druids.SegmentMetadataQueryBuilder;
import io.druid.query.Druids.SelectQueryBuilder;
import io.druid.query.filter.DimFilter;
import io.druid.query.select.PagingSpec;
import io.druid.timeline.DataSegment;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.util.EntityUtils;
import org.apache.tajo.annotation.ThreadSafe;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;

@ThreadSafe
public class DruidClient {
  public static final String DRUID_BROKER = "broker";
  public static final String DRUID_COORDINATOR = "coordinator";
  public static final String DRUID_REALTIME_SERVER = "realtime";
  public static final String DRUID_HISTORICAL_SERVER = "historical";

  public static final String ZK_SERVED_SEGMENT_PATH = "/servedSegments";
  public static final String ZK_SEGMENT_PATH = "/segments";
  public static final String ZK_ANNOUNCEMENTS_PATH = "/announcements";

  private String zkConns;
  private String zkRootPath;
  private ZooKeeper zk;

  private PoolingHttpClientConnectionManager httpConnManager;

  private Gson gson;
  private DefaultObjectMapper objectMapper;

  public DruidClient (String zkConns, String zkRootPath) throws IOException {
    this.zkConns = zkConns;
    this.zkRootPath = zkRootPath;
    this.gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
    this.httpConnManager = new PoolingHttpClientConnectionManager();
    this.objectMapper = new DefaultObjectMapper();
  }

  public List<DruidSegmentMetaData> getDruidSegmentMetas(String dataSource) throws IOException {
    try {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      return getDruidSegmentMetas(dataSource,
          DruidUtil.fromTimestamp(df.parse("1980-01-01").getTime(), false),
          DruidUtil.fromTimestamp(df.parse("2100-01-01").getTime(), false));
    } catch (ParseException e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  public List<Map<String, Object>> select(String server, String dataSource, String startInterval,
                             String endInterval, DimFilter filter) throws Exception {
    // TODO consider paging
    if (startInterval == null) {
      SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd");
      startInterval = DruidUtil.fromTimestamp(df.parse("1970-01-01").getTime(), false);
      endInterval = DruidUtil.fromTimestamp(df.parse("2099-01-01").getTime(), false);
    }

    SelectQueryBuilder builder = Druids.newSelectQueryBuilder()
        .dataSource(dataSource)
        .filters(filter)
        .granularity(QueryGranularity.DAY)
        .intervals(startInterval + "/" + endInterval)
        .pagingSpec(new PagingSpec(null, Integer.MAX_VALUE));

    List<Map<String, Object>> rows = new ArrayList<Map<String, Object>>();

    CloseableHttpClient httpClient = HttpClients.custom()
        .setConnectionManager(httpConnManager)
        .build();

    HttpPost post = new HttpPost("http://" + server + "/druid/v2/?pretty");
    post.setHeader("content-type", "application/json");
    post.setEntity(new StringEntity(objectMapper.writeValueAsString(builder.build())));
    CloseableHttpResponse response = httpClient.execute(post);

    String result = EntityUtils.toString(response.getEntity());

    JsonNode jsonNode = objectMapper.readTree(result);

    Iterator<JsonNode> fragmentResultIt = jsonNode.elements();
    while (fragmentResultIt.hasNext()) {
      JsonNode fragmentResultNode = fragmentResultIt.next();
      if (fragmentResultNode.get("result") != null && fragmentResultNode.get("result").get("events") != null) {
        Iterator<JsonNode> eventIt = fragmentResultNode.get("result").get("events").elements();
        while (eventIt.hasNext()) {
          JsonNode eachEvent = eventIt.next();
          Map<String, Object> eventValue = objectMapper.readValue(eachEvent.get("event").toString(),
              new TypeReference<Map<String, Object>>() {
              });
          rows.add(eventValue);
        }
      }
    }
    response.close();
    httpClient.close();
    return rows;
  }

  private String findBrokerIfServerNull(String server) throws IOException {
    String realServer = server;
    if (server == null) {
      //borker
      Collection<DruidServerInfo> brokers = discoverDruidServers(DRUID_BROKER);
      if (brokers.isEmpty()) {
        throw new IOException("No broker server");
      }

      DruidServerInfo druidServer = brokers.iterator().next();
      realServer = druidServer.getAddress() + ":" + druidServer.getPort();
    }

    return realServer;
  }

  public List<DruidSegmentMetaData> getDruidSegmentMetas(String dataSource,
                                                         String intervalStart,
                                                         String intervalEnd) throws IOException {
    String realServer = findBrokerIfServerNull(null);

    CloseableHttpClient httpClient = HttpClients.custom()
        .setConnectionManager(httpConnManager)
        .build();

    SegmentMetadataQueryBuilder builder = new SegmentMetadataQueryBuilder();
    builder.dataSource(dataSource).intervals(intervalStart + "/" + intervalEnd);

    HttpPost post = new HttpPost("http://" + realServer + "/druid/v2");
    post.setHeader("content-type", "application/json");
    post.setEntity(new StringEntity(objectMapper.writeValueAsString(builder.build())));
    CloseableHttpResponse response = httpClient.execute(post);
    String resultJson = EntityUtils.toString(response.getEntity());
    response.close();

    // TODO - error handling
    List<DruidSegmentMetaData> druidSegmentMetaDatas = objectMapper.readValue(resultJson,
        new TypeReference<List<DruidSegmentMetaData>>() {
        });

    setSegmentDetail(dataSource, druidSegmentMetaDatas);
    return druidSegmentMetaDatas;
  }

  public void setSegmentDetail(String dataSource, List<DruidSegmentMetaData> segmentMetas) throws IOException {
    Collection<DruidServerInfo> coordinators = discoverDruidServers(DruidClient.DRUID_COORDINATOR);
    if (coordinators.isEmpty()) {
      throw new IOException("No live coordinator server");
    }

    DruidServerInfo coordinator = coordinators.iterator().next();

    CloseableHttpClient httpClient = HttpClients.custom()
        .setConnectionManager(httpConnManager)
        .build();

    for (DruidSegmentMetaData eachSegment : segmentMetas) {
      HttpGet get = new HttpGet("http://" + coordinator.getAddress() + ":" + coordinator.getPort() +
          "/druid/coordinator/v1/datasources/" + dataSource + "/segments/" + eachSegment.getId());
      get.setHeader("content-type", "application/json");
      CloseableHttpResponse response = httpClient.execute(get);
      try {
        String result = EntityUtils.toString(response.getEntity());
        DruidSegment druidSegment = objectMapper.readValue(result, DruidSegment.class);
        eachSegment.setDataSegment(druidSegment.getMetadata());
        eachSegment.setHosts(druidSegment.getServers().toArray(new String[]{}));
      } finally {
        response.close();
      }
    }
  }

  public Collection<DruidServerInfo> discoverDruidDataServers() throws Exception {
    connectZk();

    Set<DruidServerInfo> servers = new HashSet<DruidServerInfo>();

    String servedSegementServerPath = zkRootPath + ZK_SERVED_SEGMENT_PATH;

    List<String> servedSegmentServers = zk.getChildren(servedSegementServerPath, false);
    for (String eachServer : servedSegmentServers) {
      String zkPath = zkRootPath + ZK_SEGMENT_PATH + "/" + eachServer;
      List<String> segments = zk.getChildren(zkPath, false);
      if (segments != null && !segments.isEmpty()) {
        String firstSegment = segments.get(0);
        DruidServerInfo druidServerInfo = new DruidServerInfo();
        String[] hostAndPort = eachServer.split(":");
        druidServerInfo.setAddress(hostAndPort[0]);
        druidServerInfo.setPort(Integer.parseInt(hostAndPort[1]));
        druidServerInfo.setServiceType(firstSegment.split("_")[1]);

        servers.add(druidServerInfo);
      }
    }

    return servers;
  }

  public Collection<DruidServerInfo> discoverDruidServers(String serverType) throws IOException {
    try {
      connectZk();

      List<DruidServerInfo> servers = new ArrayList<DruidServerInfo>();
      String discoverPath = zkRootPath + "/discovery/" + serverType;
      List<String> children = zk.getChildren(discoverPath, false);
      if (children != null && !children.isEmpty()) {
        Stat stat = new Stat();
        for (String eachChild : children) {
          byte[] data = zk.getData(discoverPath + "/" + eachChild, false, stat);
          Type mapType = new TypeToken<Map<String, Object>>() {
          }.getType();

          Map<String, Object> jsonMap = gson.fromJson(new String(data), mapType);
          DruidServerInfo druidServer = new DruidServerInfo();
          druidServer.setAddress((String) jsonMap.get("address"));
          druidServer.setPort(((Double) jsonMap.get("port")).intValue());
          druidServer.setServiceType(serverType);

          servers.add(druidServer);
        }
      }
      return servers;
    } catch (Exception e) {
      throw new IOException (e.getMessage(), e);
    }
  }

  private void connectZk() throws IOException, InterruptedException {
    synchronized(this) {
      if (zk == null) {
        // TODO change with curator. this is just prototype.
        final CountDownLatch connLatch = new CountDownLatch(1);
        zk = new ZooKeeper(zkConns, 10 * 1000, new Watcher() {
          @Override
          public void process(WatchedEvent watchedEvent) {
            if (watchedEvent.getType() == EventType.None) {
              if (watchedEvent.getState() == KeeperState.SyncConnected) {
                connLatch.countDown();
              }
            }
          }
        });

        connLatch.await();
      }
    }
  }

  public void close() {
    try {
      if (zk != null) {
        zk.close();
        zk = null;
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    if (httpConnManager != null) {
      httpConnManager.close();
      httpConnManager = null;
    }
  }


  static class DruidSegment {
    private DataSegment metadata;
    private List<String> servers;
    @JsonCreator
    public DruidSegment(
        @JsonProperty("metadata") DataSegment metadata,
        @JsonProperty("servers") List<String> servers) {
      this.metadata = metadata;
      this.servers = servers;
    }

    public DataSegment getMetadata() {
      return metadata;
    }

    public List<String> getServers() {
      return servers;
    }
  }
}
