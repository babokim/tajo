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

import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.util.KeyValueSet;

import java.util.List;

public class TestDruidTableSpace {
  public static void main (String[] args) throws Exception {
    Schema schema = new Schema();
    schema.addColumn("tcol1", Type.TEXT);
    schema.addColumn("tcol2", Type.FLOAT4);
    schema.addColumn("tcol3", Type.FLOAT4);
    schema.addColumn("tcol4", Type.INT8);
    schema.addColumn("timestamp_col", Type.INT8);

    KeyValueSet keyValues = new KeyValueSet();
    keyValues.set(DruidStorageConstants.META_ZOOKEEPER_QUORUM, "127.0.0.1:2181");
    keyValues.set(DruidStorageConstants.META_COLUMNS_KEY, "col1,col2,col3,num_rows,timestamp");
    keyValues.set(DruidStorageConstants.META_DATASOURCE_KEY, "test01");
    TableMeta meta = new TableMeta("druid", keyValues);

    TableDesc tableDesc = new TableDesc();
    tableDesc.setName("t_druid");
    tableDesc.setMeta(meta);
    tableDesc.setSchema(schema);

    DruidTablespace druidTablespace = new DruidTablespace("druid");
    List<Fragment> fragments = druidTablespace.getSplits("t1", tableDesc, null);

    if (fragments.isEmpty()) {
      throw new Exception("Fail no segments");
    }
    for (Fragment eachFragment: fragments) {
      System.out.println(">>>>>" + eachFragment);
    }

    for (Fragment eachFragment: fragments) {
      DruidScanner scanner = new DruidScanner(new TajoConf(),
          schema, meta, eachFragment);

      Column[] targetColumns = new Column[]{schema.getColumn("tcol3"), schema.getColumn("tcol4"),
          schema.getColumn("timestamp_col"), schema.getColumn("tcol1")};

      scanner.setTarget(targetColumns);
      scanner.init();
      Tuple tuple = null;

      System.out.println("Data from " + eachFragment);
      while ( (tuple = scanner.next()) != null ) {
        System.out.println(tuple);
      }
      scanner.close();
    }
    druidTablespace.close();
  }
}
