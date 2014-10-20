package org.apache.tajo.storage; /**
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

import org.apache.hadoop.conf.Configuration;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;

import java.io.IOException;

public class NullScanner extends AbstractScanner {
  public NullScanner(Configuration conf, Schema schema, TableMeta meta, Fragment fragment) {
    super(conf, schema, meta, fragment);
  }

  @Override
  public void init() throws IOException {
    super.init();
    this.progress = 1.0f;
  }

  @Override
  public Tuple next() throws IOException {
    return null;
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
  public boolean isSelectable() {
    return true;
  }

  @Override
  public boolean isSplittable() {
    return true;
  }
}
