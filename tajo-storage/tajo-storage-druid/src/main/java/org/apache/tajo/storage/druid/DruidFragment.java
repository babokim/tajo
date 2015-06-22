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

import com.google.gson.annotations.Expose;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.util.StringUtils;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.storage.druid.client.DruidSegmentMetaData;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.hbase.StorageFragmentProtos.DruidFragmentProto;
import org.apache.tajo.util.TUtil;

public class DruidFragment implements Fragment, Comparable<DruidFragment>, Cloneable {
  @Expose
  private String tableName;
  @Expose
  private String dataSource;
  @Expose
  private String segmentId;
  @Expose
  private long size;
  @Expose
  private boolean fromDeepStorage;
  @Expose
  private String deepStoragePath;
  @Expose
  private String[] hosts;

  public DruidFragment(String fragmentId, String dataSource, String segmentId,
                       boolean fromDeepStorage, String[] hosts) {
    this.tableName = fragmentId;
    this.dataSource = dataSource;
    this.segmentId = segmentId;
    this.hosts = hosts;
    this.fromDeepStorage = fromDeepStorage;
  }

  public DruidFragment(String fragmentId, String dataSource, boolean fromDeepStorage, DruidSegmentMetaData segment) {
    this(fragmentId, dataSource, segment.getId(), fromDeepStorage, segment.getHosts());
    this.size = segment.getSize();
  }

  public DruidFragment(ByteString raw) throws InvalidProtocolBufferException {
    DruidFragmentProto.Builder builder = DruidFragmentProto.newBuilder();
    builder.mergeFrom(raw);
    builder.build();
    init(builder.build());
  }

  private void init(DruidFragmentProto proto) {
    this.tableName = proto.getTableName();
    this.dataSource = proto.getDataSourceName();
    this.segmentId = proto.getSegmentId();
    this.size = proto.getSize();
    this.fromDeepStorage = proto.getFromDeepStorage();
    this.deepStoragePath = proto.getDeepStoragePath();
    if (proto.getHostsList() != null) {
      this.hosts = proto.getHostsList().toArray(new String[]{});
    }
  }

  @Override
  public String getTableName() {
    return tableName;
  }

  @Override
  public FragmentProto getProto() {
    DruidFragmentProto.Builder builder = DruidFragmentProto.newBuilder();
    builder.setTableName(tableName)
        .setDataSourceName(dataSource)
        .setSegmentId(segmentId)
        .setDeepStoragePath(deepStoragePath)
        .setFromDeepStorage(fromDeepStorage)
        .setSize(size);

    if(hosts != null) {
      builder.addAllHosts(TUtil.newList(hosts));
    }

    FragmentProto.Builder fragmentBuilder = FragmentProto.newBuilder();
    fragmentBuilder.setId(this.tableName);
    fragmentBuilder.setContents(builder.buildPartial().toByteString());
    fragmentBuilder.setStoreType(CatalogUtil.getStoreTypeString(StoreType.DRUID));
    return fragmentBuilder.build();
  }

  @Override
  public long getLength() {
    return size;
  }

  @Override
  public String getKey() {
    return segmentId;
  }

  @Override
  public String[] getHosts() {
    return hosts;
  }

  @Override
  public boolean isEmpty() {
    return false;
  }

  public String getDataSource() {
    return dataSource;
  }

  public boolean isFromDeepStorage() {
    return fromDeepStorage;
  }

  public String getSegmentId() {
    return segmentId;
  }

  public String getDeepStoragePath() {
    return deepStoragePath;
  }

  public void setDeepStoragePath(String deepStoragePath) {
    this.deepStoragePath = deepStoragePath;
  }

  @Override
  public int compareTo(DruidFragment o) {
    return segmentId.compareTo(o.segmentId);
  }

  public Object clone() throws CloneNotSupportedException {
    DruidFragment frag = (DruidFragment) super.clone();
    frag.tableName = tableName;
    frag.dataSource = dataSource;
    frag.segmentId = segmentId;
    frag.size = size;
    frag.hosts = hosts;
    return frag;
  }

  @Override
  public int hashCode() {
    return segmentId.hashCode();
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof DruidFragment) {
      DruidFragment t = (DruidFragment) o;
      return segmentId.equals(t.segmentId);
    }
    return false;
  }

  @Override
  public String toString() {
    return "tableName=" + tableName + ",dataSource=" + dataSource + ",segmentId=" + segmentId +
        ",fromDeepStorag=" + fromDeepStorage +
        ",hosts=" + (hosts != null ? StringUtils.arrayToString(hosts) : "null");
  }
}
