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

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.datum.*;
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.storage.druid.client.DruidClient;
import org.apache.tajo.storage.druid.client.DruidUtil;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;
import java.text.ParseException;
import java.util.Iterator;
import java.util.Map;

public class DruidQueryScanner implements Scanner {
  private Configuration conf;
  private Schema schema;
  private TableMeta meta;
  private DruidFragment fragment;
  private EvalNode qual;

  // value of array is druid column name, index of array is index of vtuple
  private String[] columnMappingIndex;
  private Type[] columnDataTypes;

  private String zkRootPath;

  public DruidQueryScanner(Configuration conf, final Schema schema, final TableMeta meta,
                           final Fragment fragment) throws Exception {
    this.conf = conf;
    this.schema = schema;
    this.meta = meta;
    this.fragment = (DruidFragment)fragment;
    this.zkRootPath = meta.getOption(DruidStorageConstants.META_ZOOKEEPER_ROOT_PATH, "/druid");

    String mappedColumns = meta.getOption(DruidStorageConstants.META_COLUMNS_KEY, "");
    String[] druidColumns = mappedColumns.split(",");

    columnMappingIndex = new String[druidColumns.length];
    columnDataTypes = new Type[druidColumns.length];
    int mappingIndex = 0;
    for (String eachDruidColumn: druidColumns) {
      columnMappingIndex[mappingIndex] = eachDruidColumn;
      columnDataTypes[mappingIndex] = schema.getColumn(mappingIndex).getDataType().getType();
      mappingIndex++;
    }
    numColumns = columnMappingIndex.length;
  }

  @Override
  public void init() throws IOException {

  }

  private Iterator<Map<String, Object>> it;
  private int numColumns;

  @Override
  public Tuple next() throws IOException {
    if (it == null) {
      try {
        DruidClient druidClient = new DruidClient("127.0.0.1:2181", zkRootPath);
        //Object[][] resultSet = druidClient.runQuery(intervalStart, intervalEnd, filters);
        String[] hosts = fragment.getHosts();
        String host = (hosts == null || hosts.length == 0) ? null : hosts[0];
        it = druidClient.select(host,
            meta.getOption(DruidStorageConstants.META_DATASOURCE_KEY), null, null, null).iterator();
      } catch (Exception e) {
        e.printStackTrace();
        throw new IOException(e.getMessage(), e);
      }
    }
    if (it.hasNext()) {
      Map<String, Object> rowData = it.next();
      Tuple tuple = new VTuple(numColumns);

      for (int i = 0; i < columnMappingIndex.length; i++) {
        tuple.put(i, makeDatum(columnMappingIndex[i], rowData.get(columnMappingIndex[i]), columnDataTypes[i]));
      }
      return tuple;
    } else {
      return null;
    }
  }

  private Datum makeDatum(String columnName, Object obj, Type type) throws IOException {
    if (type == Type.TEXT) {
      if (!(obj instanceof String)) {
        throw new IOException(columnName + " data is not String for TEXT type:" + obj.getClass());
      }
      return new TextDatum((String)obj);
    } else if (type == Type.INT4) {
      if (obj instanceof Integer) {
        return new Int4Datum((Integer) obj);
      } else if (obj instanceof Long) {
        return new Int4Datum(((Long)obj).intValue());
      } else {
        throw new IOException(columnName + " data is not Long for INT4 type: " + obj.getClass());
      }
    } else if (type == Type.INT8) {
      if (obj instanceof Integer) {
        return new Int8Datum((Integer) obj);
      } else if (obj instanceof Long) {
        return new Int8Datum((Long) obj);
      } else {
        if (columnName.equals("timestamp") && obj instanceof String) {
          try {
            return new Int8Datum(DruidUtil.toCalendar((String)obj).getTime().getTime());
          } catch (ParseException e) {
            throw new IOException(e.getMessage(), e);
          }
        }
        throw new IOException(columnName + " data is not Long for INT8 type: " + obj.getClass());
      }
    } else if (type == Type.FLOAT4) {
      if (obj instanceof Float) {
        return new Float4Datum((Float) obj);
      } else if (obj instanceof Double) {
          return new Float4Datum(((Double)obj).floatValue());
      } else {
        throw new IOException(columnName + " data is not Float for FLOAT4 type: " + obj.getClass());
      }
    } else if (type == Type.FLOAT8) {
      if (obj instanceof Float) {
        return new Float8Datum((Float)obj);
      } else if (obj instanceof Double) {
        return new Float8Datum((Double)obj);
      } else {
        throw new IOException(columnName + " data is not Float for FLOAT8 type: " + obj.getClass());
      }
    } else {
      throw new IOException(columnName + " is wrong data type[" + type + "]");
    }
  }

  @Override
  public void reset() throws IOException {

  }

  @Override
  public void close() throws IOException {

  }

  @Override
  public boolean isProjectable() {
    return false;
  }

  @Override
  public void setTarget(Column[] targets) {
    //TODO apply this column to dimension property in druid query
  }

  @Override
  public boolean isSelectable() {
    return false;
  }

  @Override
  public void setSearchCondition(Object expr) {
    qual = (EvalNode)expr;
  }

  @Override
  public boolean isSplittable() {
    return false;
  }

  @Override
  public float getProgress() {
    return 0;
  }

  @Override
  public TableStats getInputStats() {
    return null;
  }

  @Override
  public Schema getSchema() {
    return null;
  }
}
