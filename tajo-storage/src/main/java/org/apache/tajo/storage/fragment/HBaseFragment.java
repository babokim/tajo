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

package org.apache.tajo.storage.fragment;

import com.google.common.base.Objects;
import com.google.gson.annotations.Expose;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.index.IndexProtos.HBaseFragmentProto;

public class HBaseFragment implements Fragment, Comparable<HBaseFragment>, Cloneable {
  @Expose
  private String id;
  @Expose
  private String hbaseTableName;
  @Expose
  private byte[] startRow;
  @Expose
  private byte[] stopRow;
  @Expose
  private String regionLocation;

  public HBaseFragment(String id, String hbaseTableName, byte[] startRow, byte[] stopRow, String regionLocation) {
    this.id = id;
    this.hbaseTableName = hbaseTableName;
    this.startRow = startRow;
    this.stopRow = stopRow;
    this.regionLocation = regionLocation;
  }

  public HBaseFragment(ByteString raw) throws InvalidProtocolBufferException {
    HBaseFragmentProto.Builder builder = HBaseFragmentProto.newBuilder();
    builder.mergeFrom(raw);
    builder.build();
    init(builder.build());
  }

  private void init(HBaseFragmentProto proto) {
    this.id = proto.getId();
    this.hbaseTableName = proto.getHbaseTableName();
    this.startRow = proto.getStartRow().toByteArray();
    this.stopRow = proto.getStopRow().toByteArray();
    this.regionLocation = proto.getRegionLocation();
  }

  @Override
  public int compareTo(HBaseFragment t) {
    return Bytes.compareTo(startRow, t.startRow);
  }

  @Override
  public String getHbaseTableName() {
    return hbaseTableName;
  }

  @Override
  public String getKey() {
    return new String(startRow);
  }

  @Override
  public boolean isEmpty() {
    return startRow == null || stopRow == null;
  }

  @Override
  public long getNumBytes() {
    return 0;
  }

  @Override
  public String[] getHosts() {
    return new String[] {regionLocation};
  }

  public Object clone() throws CloneNotSupportedException {
    HBaseFragment frag = (HBaseFragment) super.clone();
    frag.id = id;
    frag.hbaseTableName = hbaseTableName;
    frag.startRow = startRow;
    frag.stopRow = stopRow;
    frag.regionLocation = regionLocation;

    return frag;
  }

  @Override
  public boolean equals(Object o) {
    if (o instanceof HBaseFragment) {
      HBaseFragment t = (HBaseFragment) o;
      if (id.equals(t.id)
          && Bytes.equals(startRow, t.startRow)
          && Bytes.equals(stopRow, t.stopRow)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(id, hbaseTableName, startRow, stopRow);
  }

  @Override
  public String toString() {
    return "\"fragment\": {\"id\": \""+ id + "\", hbaseTableName\": " + hbaseTableName + "\"" +
        ", \"startRow\": " + new String(startRow) +
        ", \"stopRow\": " + new String(stopRow) + "}" ;
  }

  @Override
  public FragmentProto getProto() {
    HBaseFragmentProto.Builder builder = HBaseFragmentProto.newBuilder();
    builder.setId(id)
        .setHbaseTableName(hbaseTableName)
        .setStartRow(ByteString.copyFrom(startRow))
        .setStopRow(ByteString.copyFrom(stopRow))
        .setRegionLocation(regionLocation);

    FragmentProto.Builder fragmentBuilder = FragmentProto.newBuilder();
    fragmentBuilder.setId(this.id);
    fragmentBuilder.setContents(builder.buildPartial().toByteString());
    return fragmentBuilder.build();
  }

  public byte[] getStartRow() {
    return startRow;
  }

  public byte[] getStopRow() {
    return stopRow;
  }

  public String getRegionLocation() {
    return regionLocation;
  }
}
