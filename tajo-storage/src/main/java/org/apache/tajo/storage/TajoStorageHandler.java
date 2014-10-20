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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.TableDesc;
import org.apache.tajo.catalog.TableMeta;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.catalog.proto.CatalogProtos.StoreType;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.Fragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public abstract class TajoStorageHandler {
  /**
   * Cache of TajoStorageHandler instance
   */
  private static Map<String, TajoStorageHandler> storageHandlerMap = new HashMap<String, TajoStorageHandler>();

  /**
   * Cache of scanner handlers for each storage type.
   */
  protected static final Map<String, Class<? extends Scanner>> SCANNER_HANDLER_CACHE
      = new ConcurrentHashMap<String, Class<? extends Scanner>>();

  /**
   * Cache of appender handlers for each storage type.
   */
  protected static final Map<String, Class<? extends FileAppender>> APPENDER_HANDLER_CACHE
      = new ConcurrentHashMap<String, Class<? extends FileAppender>>();

  /**
   * Cache of constructors for each class. Pins the classes so they
   * can't be garbage collected until ReflectionUtils can be collected.
   */
  private static final Map<Class<?>, Constructor<?>> CONSTRUCTOR_CACHE =
      new ConcurrentHashMap<Class<?>, Constructor<?>>();

  protected TajoConf tajoConf;

  public abstract void createTable(TableDesc tableDesc) throws IOException;
  public abstract void purgeTable(TableDesc tableDesc) throws IOException;
  public abstract List<Fragment> getFragments(String fragmentId, TableDesc tableDesc) throws IOException;

  protected abstract void handlerInit() throws IOException ;

  public void init(TajoConf tajoConf) throws IOException {
    this.tajoConf = tajoConf;
    handlerInit();
  }

  public static FileStorageHandler getFileStorageHandler(TajoConf tajoConf) throws IOException {
    return getFileStorageHandler(tajoConf, null);
  }

  public static FileStorageHandler getFileStorageHandler(TajoConf tajoConf, Path warehousePath) throws IOException {
    URI uri;
    TajoConf copiedConf = new TajoConf(tajoConf);
    if (warehousePath != null) {
      copiedConf.setVar(ConfVars.WAREHOUSE_DIR, warehousePath.toUri().toString());
    }
    uri = TajoConf.getWarehouseDir(copiedConf).toUri();
    String key = "file".equals(uri.getScheme()) ? "file" : uri.toString();
    return (FileStorageHandler)TajoStorageHandler.getStorageHandler(copiedConf, StoreType.CSV, key);
  }

  public static TajoStorageHandler getStorageHandler(TajoConf tajoConf, StoreType storeType) throws IOException {
    return getStorageHandler(tajoConf, storeType, null);
  }

  public static TajoStorageHandler getStorageHandler(TajoConf tajoConf, StoreType storeType, String key) throws IOException {
    synchronized (storageHandlerMap) {
      String storeKey = storeType + key;
      TajoStorageHandler handler = storageHandlerMap.get(storeKey);
      if (handler == null) {
        switch (storeType) {
          case HBASE:
            handler = new HBaseStorageHandler();
            break;
          default:
            handler = new FileStorageHandler();
        }

        handler.init(tajoConf);
        storageHandlerMap.put(storeKey, handler);
      }

      return handler;
    }
  }

  public static synchronized SeekableScanner getSeekableScanner(
      TajoConf conf, TableMeta meta, Schema schema, FileFragment fragment, Schema target) throws IOException {
    return (SeekableScanner)getStorageHandler(conf, meta.getStoreType()).getScanner(meta, schema, fragment, target);
  }

  public static synchronized SeekableScanner getSeekableScanner(
      TajoConf conf, TableMeta meta, Schema schema, Path path) throws IOException {

    FileSystem fs = path.getFileSystem(conf);
    FileStatus status = fs.getFileStatus(path);
    FileFragment fragment = new FileFragment(path.getName(), path, 0, status.getLen());

    return getSeekableScanner(conf, meta, schema, fragment, schema);
  }

  public Class<? extends Scanner> getScannerClass(CatalogProtos.StoreType storeType) throws IOException {
    String handlerName = storeType.name().toLowerCase();
    Class<? extends Scanner> scannerClass = SCANNER_HANDLER_CACHE.get(handlerName);
    if (scannerClass == null) {
      scannerClass = tajoConf.getClass(
          String.format("tajo.storage.scanner-handler.%s.class",storeType.name().toLowerCase()), null, Scanner.class);
      SCANNER_HANDLER_CACHE.put(handlerName, scannerClass);
    }

    if (scannerClass == null) {
      throw new IOException("Unknown Storage Type: " + storeType.name());
    }

    return scannerClass;
  }

  public Scanner getScanner(TableMeta meta, Schema schema, Fragment fragment, Schema target) throws IOException {
    if (fragment.isEmpty()) {
      Scanner scanner = new NullScanner(tajoConf, schema, meta, fragment);
      scanner.setTarget(target.toArray());

      return scanner;
    }

    Scanner scanner;

    Class<? extends Scanner> scannerClass = getScannerClass(meta.getStoreType());
    scanner = newScannerInstance(scannerClass, tajoConf, schema, meta, fragment);
    if (scanner.isProjectable()) {
      scanner.setTarget(target.toArray());
    }

    return scanner;
  }

  public Scanner getScanner(TableMeta meta, Schema schema, FragmentProto fragment) throws IOException {
    return getScanner(meta, schema, FragmentConvertor.convert(tajoConf, meta.getStoreType(), fragment), schema);
  }

  public Scanner getScanner(TableMeta meta, Schema schema, FragmentProto fragment, Schema target) throws IOException {
    return getScanner(meta, schema, FragmentConvertor.convert(tajoConf, meta.getStoreType(), fragment), target);
  }

  public Scanner getScanner(TableMeta meta, Schema schema, Fragment fragment) throws IOException {
    return getScanner(meta, schema, fragment, schema);
  }

  public Appender getAppender(TableMeta meta, Schema schema, Path path)
      throws IOException {
    Appender appender;

    Class<? extends FileAppender> appenderClass;

    String handlerName = meta.getStoreType().name().toLowerCase();
    appenderClass = APPENDER_HANDLER_CACHE.get(handlerName);
    if (appenderClass == null) {
      appenderClass = tajoConf.getClass(
          String.format("tajo.storage.appender-handler.%s.class",
              meta.getStoreType().name().toLowerCase()), null,
          FileAppender.class);
      APPENDER_HANDLER_CACHE.put(handlerName, appenderClass);
    }

    if (appenderClass == null) {
      throw new IOException("Unknown Storage Type: " + meta.getStoreType());
    }

    appender = newAppenderInstance(appenderClass, tajoConf, meta, schema, path);

    return appender;
  }

  public TajoConf getConf() {
    return tajoConf;
  }

  private static final Class<?>[] DEFAULT_SCANNER_PARAMS = {
      Configuration.class,
      Schema.class,
      TableMeta.class,
      Fragment.class
  };

  private static final Class<?>[] DEFAULT_APPENDER_PARAMS = {
      Configuration.class,
      Schema.class,
      TableMeta.class,
      Path.class
  };

  /**
   * create a scanner instance.
   */
  public static <T> T newScannerInstance(Class<T> theClass, Configuration conf, Schema schema, TableMeta meta,
                                         Fragment fragment) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(DEFAULT_SCANNER_PARAMS);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(new Object[]{conf, schema, meta, fragment});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result;
  }

  /**
   * create a scanner instance.
   */
  public static <T> T newAppenderInstance(Class<T> theClass, Configuration conf, TableMeta meta, Schema schema,
                                          Path path) {
    T result;
    try {
      Constructor<T> meth = (Constructor<T>) CONSTRUCTOR_CACHE.get(theClass);
      if (meth == null) {
        meth = theClass.getDeclaredConstructor(DEFAULT_APPENDER_PARAMS);
        meth.setAccessible(true);
        CONSTRUCTOR_CACHE.put(theClass, meth);
      }
      result = meth.newInstance(new Object[]{conf, schema, meta, path});
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return result;
  }
}
