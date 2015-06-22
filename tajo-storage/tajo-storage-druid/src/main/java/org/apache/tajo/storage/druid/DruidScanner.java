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
import org.apache.tajo.plan.expr.EvalNode;
import org.apache.tajo.storage.Scanner;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;

public class DruidScanner implements Scanner {
  /**
   * If time interval in where clause is under configured value(defalut: 10 days) use DruadFileScanner,
   * Else use DraudQeuryScanner
   */
  private Scanner scanner;
  private Configuration conf;
  private Schema schema;
  private TableMeta meta;
  private DruidFragment fragment;

  private EvalNode qual;

  public DruidScanner(Configuration conf, final Schema schema, final TableMeta meta, final Fragment fragment) {
    this.conf = conf;
    this.schema = schema;
    this.meta = meta;
    this.fragment = (DruidFragment)fragment;
  }

  @Override
  public void init() throws IOException {
    try {
      if (fragment.isFromDeepStorage()) {
        scanner = new DruidFileScanner(conf, schema, meta, fragment);
      } else {
        scanner = new DruidQueryScanner(conf, schema, meta, fragment);
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    if (qual != null) {
      scanner.setSearchCondition(qual);
    }
    scanner.init();
  }

  @Override
  public Tuple next() throws IOException {
    return scanner.next();
  }

  @Override
  public void reset() throws IOException {

  }

  @Override
  public void close() throws IOException {
    scanner.close();
  }

  @Override
  public boolean isProjectable() {
    return false;
  }

  @Override
  public void setTarget(Column[] targets) {

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
