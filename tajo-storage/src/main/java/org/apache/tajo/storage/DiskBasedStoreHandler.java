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

package org.apache.tajo.storage;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.catalog.CatalogUtil;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.conf.TajoConf;

import java.io.IOException;

public class DiskBasedStoreHandler implements TajoStorageHandler {
  private final Log LOG = LogFactory.getLog(DiskBasedStoreHandler.class);

  @Override
  public void createTable(TajoConf tajoConf, AbstractStorageManager sm, TableDesc tableDesc) throws IOException {
    if (!tableDesc.isExternal()) {
      String [] splitted = CatalogUtil.splitFQTableName(tableDesc.getName());
      String databaseName = splitted[0];
      String simpleTableName = splitted[1];

      // create a table directory (i.e., ${WAREHOUSE_DIR}/${DATABASE_NAME}/${TABLE_NAME} )
      Path tablePath = StorageUtil.concatPath(sm.getWarehouseDir(), databaseName, simpleTableName);
      tableDesc.setPath(tablePath);
    } else {
      Preconditions.checkState(tableDesc.getPath() != null, "ERROR: LOCATION must be given.");
    }

    Path path = tableDesc.getPath();

    FileSystem fs = path.getFileSystem(tajoConf);
    TableStats stats = new TableStats();
    if (tableDesc.isExternal()) {
      if (!fs.exists(path)) {
        LOG.error(path.toUri() + " does not exist");
        throw new IOException("ERROR: " + path.toUri() + " does not exist");
      }
    } else {
      fs.mkdirs(path);
    }

    long totalSize = 0;

    try {
      totalSize = sm.calculateSize(path);
    } catch (IOException e) {
      LOG.warn("Cannot calculate the size of the relation", e);
    }

    stats.setNumBytes(totalSize);

    if (tableDesc.isExternal()) { // if it is an external table, there is no way to know the exact row number without processing.
      stats.setNumRows(TajoConstants.UNKNOWN_ROW_NUMBER);
    }

    tableDesc.setStats(stats);
  }

  @Override
  public void purgeTable(TajoConf tajoConf, TableDesc tableDesc) throws IOException {
    try {
      Path path = tableDesc.getPath();
      FileSystem fs = path.getFileSystem(tajoConf);
      fs.delete(path, true);
    } catch (IOException e) {
      throw new InternalError(e.getMessage());
    }
  }
}
