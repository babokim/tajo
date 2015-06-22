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

import com.google.common.base.Preconditions;
import com.metamx.common.guava.Accumulator;
import com.metamx.common.guava.Sequence;
import io.druid.granularity.QueryGranularity;
import io.druid.hdfs.IndexHdfsIO;
import io.druid.hdfs.SimpleQueryableHdfsIndex;
import io.druid.segment.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.ColumnStats;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DruidFileScanner implements Scanner {
  private static final Log LOG = LogFactory.getLog(DruidFileScanner.class);

  protected boolean inited = false;

  private TajoConf conf;
  private Schema schema;
  private Column [] targets;
  private TableMeta meta;

  private TableStats tableStats;
  private DruidFragment fragment;

  //druid column name -> tajo result typle target index
  private Map<String, Integer> targetIndexes;
  private int timestampFieldIndex = -1;

  private DruidCursorIterator cursorIt;

  private SimpleQueryableHdfsIndex druidIndex;

  private long numReadRow;
  private long numRows;

  public DruidFileScanner (Configuration conf, Schema schema, TableMeta meta, Fragment fragment) throws IOException {
    Preconditions.checkNotNull(conf);
    Preconditions.checkNotNull(schema);
    Preconditions.checkNotNull(meta);
    Preconditions.checkNotNull(fragment);
    Preconditions.checkArgument(conf instanceof TajoConf);

    this.conf = (TajoConf) conf;
    this.schema = schema;
    this.meta = meta;
    this.fragment = (DruidFragment)fragment;
    this.tableStats = new TableStats();
  }

  @Override
  public void setTarget(Column[] targets) {
    if (inited) {
      throw new IllegalStateException("Should be called before init()");
    }
    this.targets = targets;
  }

  @Override
  public boolean isSelectable() {
    return true;
  }

  @Override
  public void setSearchCondition(Object expr) {

  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public float getProgress() {
    if (numRows == 0) {
      return 1.0f;
    }
    return numReadRow/numRows;
  }

  @Override
  public TableStats getInputStats() {
    return tableStats;
  }

  @Override
  public void init() throws IOException {
    if (!inited) {
      inited = true;
      if (fragment != null) {
        tableStats.setNumBytes(0);
        tableStats.setNumBlocks(1);
      }

      // tajo column -> druid column
      Map<String, String> columnMapping = new HashMap<String, String>();
      String[] druidColumns = meta.getOption(DruidStorageConstants.META_COLUMNS_KEY).split(",");

      int index = 0;
      String timestampColumnName = null;
      for (Column eachColumn : schema.getRootColumns()) {
        columnMapping.put(eachColumn.getSimpleName(), druidColumns[index]);
        if (druidColumns[index].equals(DruidStorageConstants.TIMESTAMP_COLUMN)) {
          timestampColumnName = eachColumn.getSimpleName();
        }
        ColumnStats columnStats = new ColumnStats(eachColumn);
        tableStats.addColumnStat(columnStats);
        index++;
      }

      if (targets == null) {
        targets = schema.toArray();
      }
      targetIndexes = new HashMap<String, Integer>();

      index = 0;
      for (Column eachTargetColumn: targets) {
        String druidColumn = columnMapping.get(eachTargetColumn.getSimpleName());
        targetIndexes.put(druidColumn, index);
        if (eachTargetColumn.getSimpleName().equals(timestampColumnName)) {
          timestampFieldIndex = index;
        }
        index++;
      }
    }
    initScanner();
  }

  @Override
  public Tuple next() throws IOException {
    return cursorIt.next();
  }

  @Override
  public void reset() throws IOException {
    close();
    druidIndex = null;
    initScanner();
  }

  @Override
  public void close() throws IOException {
    if (druidIndex != null) {
      druidIndex.close();
    }
  }

  @Override
  public boolean isProjectable() {
    return true;
  }

  private void initScanner() throws IOException {
    druidIndex =
        (SimpleQueryableHdfsIndex) IndexHdfsIO.loadIndex(conf, (new Path(fragment.getDeepStoragePath())));

    QueryableIndexIndexableAdapter indexAdapter = new QueryableIndexIndexableAdapter(druidIndex);

    final List<String> dims = new ArrayList<String>();
    for (String eachDim: indexAdapter.getDimensionNames()) {
      dims.add(eachDim);
    }

    final List<String> metrics = new ArrayList<String>();
    for (String eachMetric: indexAdapter.getMetricNames()) {
      metrics.add(eachMetric);
    }

    QueryableIndexStorageAdapter adapter = new QueryableIndexStorageAdapter(druidIndex);
    Sequence<Cursor> sequence = adapter.makeCursors(null, druidIndex.getDataInterval(), QueryGranularity.SECOND);

    cursorIt = sequence.accumulate(
      new DruidCursorIterator(targetIndexes, dims, metrics, timestampFieldIndex),
      new Accumulator<DruidCursorIterator, Cursor>() {
        @Override
        public DruidCursorIterator accumulate(DruidCursorIterator it, Cursor cursor) {
          it.setCursor(cursor);
          return it;
        }
      }
    );

    numRows = druidIndex.getNumRows();
    numReadRow = 0;
  }

  @Override
  public Schema getSchema() {
    return schema;
  }
}
