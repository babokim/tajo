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

import org.apache.hadoop.fs.Path;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.*;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.plan.LogicalPlan;
import org.apache.tajo.plan.logical.LogicalNode;
import org.apache.tajo.plan.logical.ScanNode;
import org.apache.tajo.plan.rewrite.LogicalPlanRewriteRule;
import org.apache.tajo.storage.StorageProperty;
import org.apache.tajo.storage.Tablespace;
import org.apache.tajo.storage.TupleRange;
import org.apache.tajo.storage.druid.client.DruidClient;
import org.apache.tajo.storage.druid.client.DruidColumn;
import org.apache.tajo.storage.druid.client.DruidSegmentMetaData;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DruidTablespace extends Tablespace {
  private StorageProperty storageProperty;
  private DruidClient druidClient;

  public DruidTablespace(String storeType) {
    super(storeType);
    storageProperty = new StorageProperty();
    storageProperty.setSortedInsert(false);
    storageProperty.setSupportsInsertInto(false);
  }

  @Override
  protected void storageInit() throws IOException {

  }

  @Override
  public void createTable(TableDesc tableDesc, boolean ifNotExists) throws IOException {
    createTable(tableDesc.getMeta(), tableDesc.getSchema(), tableDesc.isExternal(), ifNotExists);
    TableStats stats = new TableStats();
    stats.setNumRows(TajoConstants.UNKNOWN_ROW_NUMBER);
    tableDesc.setStats(stats);
  }

  /**
   * Druid can't support create data source directly. This method only verifies datasource name and
   * column mappings.
   * @param tableMeta
   * @param schema
   * @param isExternal
   * @param ifNotExists
   * @throws IOException
   */
  private void createTable(TableMeta tableMeta, Schema schema,
                           boolean isExternal, boolean ifNotExists) throws IOException {
    String druidDataSource = tableMeta.getOption(DruidStorageConstants.META_DATASOURCE_KEY, "");
    if (druidDataSource == null || druidDataSource.trim().isEmpty()) {
      throw new IOException("Druid mapped table is required a '" +
          DruidStorageConstants.META_DATASOURCE_KEY + "' attribute.");
    }

    String zkQuorums = tableMeta.getOption(DruidStorageConstants.META_ZOOKEEPER_QUORUM, "");
    if (zkQuorums == null || zkQuorums.trim().isEmpty()) {
      throw new IOException("Druid mapped table is required a '" +
          DruidStorageConstants.META_ZOOKEEPER_QUORUM + "' attribute.");
    }

    String mappedColumns = tableMeta.getOption(DruidStorageConstants.META_COLUMNS_KEY, "");
    if (mappedColumns != null && mappedColumns.split(",").length > schema.size()) {
      throw new IOException("Columns property has more entry than Tajo table columns");
    }

    DruidClient druidClient = getDruidClient(tableMeta);
    List<DruidSegmentMetaData> segments = druidClient.getDruidSegmentMetas(druidDataSource);
    if (segments.isEmpty()) {
      throw new IOException("No segments");
    }
    DruidSegmentMetaData segment = segments.get(0);
    Map<String, DruidColumn> columns = segment.getColumns();

    boolean includeTimeColumn = false;
    int index = 0;
    for (String eachColumn: mappedColumns.split(",")) {
      eachColumn = eachColumn.trim();
      if (eachColumn.equals(DruidStorageConstants.TIMESTAMP_COLUMN)) {
        includeTimeColumn = true;
        continue;
      }
      if (!columns.containsKey(eachColumn)) {
        throw new IOException(eachColumn + " not exists in druid.");
      }

      DruidColumn druidColumn = columns.get(eachColumn);
      Column tajoColumn = schema.getColumn(index);
      if (druidColumn.getType().equalsIgnoreCase("string")) {
        if (tajoColumn.getDataType().getType() != Type.TEXT) {
          throw new IOException("Tajo data type [" + tajoColumn.getDataType().getType() + "] " +
              "can't map to Druid column type [" + druidColumn.getType() + "] ");
        }
      } else if (druidColumn.getType().equalsIgnoreCase("long")) {
        if (tajoColumn.getDataType().getType() != Type.INT8) {
          throw new IOException("Tajo data type [" + tajoColumn.getDataType().getType() + "] " +
              "can't map to Druid column type [" + druidColumn.getType() + "] ");
        }
      } else if (druidColumn.getType().equalsIgnoreCase("float")) {
        if (tajoColumn.getDataType().getType() != Type.FLOAT4 &&
            tajoColumn.getDataType().getType() != Type.FLOAT8) {
          throw new IOException("Tajo data type [" + tajoColumn.getDataType().getType() + "] " +
              "can't map to Druid column type [" + druidColumn.getType() + "] ");
        }
      } else {
        throw new IOException("Druid column type [" + druidColumn.getType() + "] is not defined for column mapping.");
      }
      index++;
    }

    if (!includeTimeColumn) {
      throw new IOException("No timestamp column mapping.");
    }
  }

  @Override
  public void purgeTable(TableDesc tableDesc) throws IOException {

  }

  private synchronized DruidClient getDruidClient(TableMeta tableMeta) throws IOException {
    if (druidClient == null) {
      if (tableMeta.getOption(DruidStorageConstants.META_ZOOKEEPER_QUORUM) == null) {
        throw new IOException("No " + DruidStorageConstants.META_ZOOKEEPER_QUORUM + " properties in table meta.");
      }
      druidClient = new DruidClient(
          tableMeta.getOption(DruidStorageConstants.META_ZOOKEEPER_QUORUM),
          tableMeta.getOption(DruidStorageConstants.META_ZOOKEEPER_ROOT_PATH, "/druid"));
    }

    return druidClient;
  }

  @Override
  public List<Fragment> getSplits(String fragmentId, TableDesc tableDesc, ScanNode scanNode) throws IOException {
    // TODO - If time interval is less than configured value, call to broker.
    // In current implementation, Tablespace doesn't know about Session value.

    boolean fromDeepStorage = true;
//    if (timeIntervalGap < confIntervalGap) {
//      fromDeepStorage = false;
//    }

    TableMeta tableMeta = tableDesc.getMeta();
    String druidDataSource = tableMeta.getOption(DruidStorageConstants.META_DATASOURCE_KEY, "");

    DruidClient druidClient = getDruidClient(tableMeta);
    List<Fragment> fragments = new ArrayList<Fragment>();


    List<DruidSegmentMetaData> segments = druidClient.getDruidSegmentMetas(druidDataSource);

    for (DruidSegmentMetaData eachSegment: segments) {
      // TODO if scanNode has interval filter, ignore segments which are out of interval range.

      boolean eachFromDeepStorage = fromDeepStorage;
      String deepStoragePath = null;
      if (fromDeepStorage) {
        if (eachSegment.getDataSegment() != null && (eachSegment.getDataSegment().getLoadSpec() == null ||
            eachSegment.getDataSegment().getLoadSpec().isEmpty())) {
          eachFromDeepStorage = false;
        } else {
          deepStoragePath = eachSegment.getDataSegment().getLoadSpec().get("path").toString();
        }
      }
      DruidFragment fragment = new DruidFragment(fragmentId, druidDataSource, eachFromDeepStorage, eachSegment);
      fragment.setDeepStoragePath(deepStoragePath);
      //TODO get HDFS block info and set hosts
      fragments.add(fragment);
    }

    return fragments;
  }

  @Override
  public List<Fragment> getNonForwardSplit(TableDesc tableDesc, int currentPage, int numFragments) throws IOException {
    List<Fragment> fragments = getSplits(tableDesc.getName(), tableDesc, null);
    int start = (currentPage - 1) * numFragments;
    int end = Math.min(fragments.size() - 1, start + numFragments);

    return fragments.subList(start, end);
  }

  @Override
  public StorageProperty getStorageProperty() {
    return storageProperty;
  }

  @Override
  public void close() {
    if (druidClient != null) {
      druidClient.close();
    }
  }

  @Override
  public TupleRange[] getInsertSortRanges(OverridableConf queryContext,
                                          TableDesc tableDesc, Schema inputSchema, SortSpec[] sortSpecs, TupleRange dataRange) throws IOException {
    return new TupleRange[0];
  }

  @Override
  public void beforeInsertOrCATS(LogicalNode node) throws IOException {

  }

  @Override
  public void rollbackOutputCommit(LogicalNode node) throws IOException {

  }

  @Override
  public void verifyInsertTableSchema(TableDesc tableDesc, Schema outSchema) throws IOException {
  }

  @Override
  public List<LogicalPlanRewriteRule> getRewriteRules(OverridableConf queryContext, TableDesc tableDesc) throws IOException {
    return null;
  }

  @Override
  public Path commitOutputData(OverridableConf queryContext, ExecutionBlockId finalEbId,
                               LogicalPlan plan, Schema schema, TableDesc tableDesc) throws IOException {
    throw new IOException("Not supported operation");
  }
}
