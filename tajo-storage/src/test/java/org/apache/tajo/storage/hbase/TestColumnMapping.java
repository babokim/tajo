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

package org.apache.tajo.storage.hbase;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.storage.HBaseStorageHandler;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestColumnMapping {
  @Test
  public void testBasicColumnMapping() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.TEXT);
    schema.addColumn("col2", Type.TEXT);
    schema.addColumn("col3", Type.TEXT);

    KeyValueSet options = new KeyValueSet();
    options.set(HBaseStorageHandler.META_TABLE_KEY, "hbase_table");
    options.set(HBaseStorageHandler.META_COLUMNS_KEY, ":key,hcol2:,hcol3:#b");

    TableMeta tableMeta = new TableMeta(StoreType.HBASE, options);

    ColumnMapping columnMapping = new ColumnMapping(schema, tableMeta);

    byte[][][] mappingColumns = columnMapping.getMappingColumns();
    assertNotNull(mappingColumns);
    assertEquals(3, mappingColumns.length);

    assertEquals("hcol2", new String(mappingColumns[1][0]));
    assertNull(mappingColumns[1][1]);
    assertEquals("hcol3", new String(mappingColumns[2][0]));
    assertNull(mappingColumns[2][1]);

    boolean[] isBinaryColumns = columnMapping.getIsBinaryColumns();
    assertEquals(3, isBinaryColumns.length);
    assertFalse(isBinaryColumns[0]);
    assertFalse(isBinaryColumns[1]);
    assertTrue(isBinaryColumns[2]);

    boolean[] isRowKeyMappings = columnMapping.getIsRowKeyMappings();
    assertEquals(3, isRowKeyMappings.length);
    assertTrue(isRowKeyMappings[0]);
    assertFalse(isRowKeyMappings[1]);
    assertFalse(isRowKeyMappings[2]);
  }

  @Test
  public void testColumnNameMapping() throws Exception {
    Schema schema = new Schema();
    schema.addColumn("col1", Type.TEXT);
    schema.addColumn("col2", Type.TEXT);
    schema.addColumn("col3", Type.TEXT);
    schema.addColumn("col4", Type.TEXT);
    schema.addColumn("col5", Type.TEXT);

    KeyValueSet options = new KeyValueSet();
    options.set(HBaseStorageHandler.META_TABLE_KEY, "hbase_table");
    options.set(HBaseStorageHandler.META_COLUMNS_KEY, "0:key,1:key#b,hcol1:a,hcol2:,hcol3:b#b");
    options.set(HBaseStorageHandler.META_ROWKEY_DELIMITER, "_");
    TableMeta tableMeta = new TableMeta(StoreType.HBASE, options);

    ColumnMapping columnMapping = new ColumnMapping(schema, tableMeta);

    byte[][][] mappingColumns = columnMapping.getMappingColumns();
    assertNotNull(mappingColumns);
    assertEquals(5, mappingColumns.length);

    assertEquals("hcol1", new String(mappingColumns[2][0]));
    assertEquals("a", new String(mappingColumns[2][1]));
    assertEquals("hcol2", new String(mappingColumns[3][0]));
    assertNull(mappingColumns[3][1]);
    assertEquals("hcol3", new String(mappingColumns[4][0]));
    assertEquals("b", new String(mappingColumns[4][1]));

    boolean[] isBinaryColumns = columnMapping.getIsBinaryColumns();
    assertEquals(5, isBinaryColumns.length);
    assertFalse(isBinaryColumns[0]);
    assertTrue(isBinaryColumns[1]);
    assertFalse(isBinaryColumns[2]);
    assertFalse(isBinaryColumns[3]);
    assertTrue(isBinaryColumns[4]);

    boolean[] isRowKeyMappings = columnMapping.getIsRowKeyMappings();
    assertEquals(5, isRowKeyMappings.length);
    assertTrue(isRowKeyMappings[0]);
    assertTrue(isRowKeyMappings[1]);
    assertFalse(isRowKeyMappings[2]);
    assertFalse(isRowKeyMappings[3]);
    assertFalse(isRowKeyMappings[4]);
  }
}
