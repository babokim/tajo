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

package org.apache.tajo.engine.planner.physical;

import org.apache.hadoop.io.IOUtils;
import org.apache.tajo.catalog.Column;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.catalog.partition.PartitionMethodDesc;
import org.apache.tajo.catalog.proto.CatalogProtos;
import org.apache.tajo.catalog.proto.CatalogProtos.FragmentProto;
import org.apache.tajo.catalog.statistics.TableStats;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.engine.codegen.CompilationError;
import org.apache.tajo.engine.eval.ConstEval;
import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.eval.EvalTreeUtil;
import org.apache.tajo.engine.eval.FieldEval;
import org.apache.tajo.engine.planner.Projector;
import org.apache.tajo.engine.planner.Target;
import org.apache.tajo.engine.planner.logical.ScanNode;
import org.apache.tajo.catalog.SchemaUtil;
import org.apache.tajo.engine.utils.TupleCache;
import org.apache.tajo.engine.utils.TupleCacheKey;
import org.apache.tajo.engine.utils.TupleUtil;
import org.apache.tajo.storage.*;
import org.apache.tajo.storage.fragment.FileFragment;
import org.apache.tajo.storage.fragment.FragmentConvertor;
import org.apache.tajo.util.StopWatch;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;


public class SeqScanExec extends PhysicalExec {
  private ScanNode plan;

  private Scanner scanner = null;

  private EvalNode qual = null;

  private CatalogProtos.FragmentProto [] fragments;

  private Projector projector;

  private TableStats inputStats;

  private TupleCacheKey cacheKey;

  private boolean cacheRead = false;

  private String evalNodeKey;
  private String projectorEvalNodeKey;

  public SeqScanExec(TaskAttemptContext context, AbstractStorageManager sm, ScanNode plan,
                     CatalogProtos.FragmentProto [] fragments) throws IOException {
    super(context, plan.getInSchema(), plan.getOutSchema());

    this.plan = plan;
    this.qual = plan.getQual();
    this.fragments = fragments;

    if (plan.isBroadcastTable()) {
      String pathNameKey = "";
      if (fragments != null) {
        for (FragmentProto f : fragments) {
          FileFragment fileFragement = FragmentConvertor.convert(
              context.getConf(), plan.getTableDesc().getMeta().getStoreType(), f);
          pathNameKey += fileFragement.getPath();
        }
      }

      cacheKey = new TupleCacheKey(
          context.getTaskId().getQueryUnitId().getExecutionBlockId().toString(), plan.getTableName(), pathNameKey);
    }

    if (fragments != null
        && plan.getTableDesc().hasPartition()
        && plan.getTableDesc().getPartitionMethod().getPartitionType() == CatalogProtos.PartitionType.COLUMN) {
      rewriteColumnPartitionedTableSchema();
    }

    if (this.qual != null) {
      evalNodeKey = this.getClass().getSimpleName() + "_" + plan.getPID() + "." + this.qual.getClass().getSimpleName();
    }

    projectorEvalNodeKey = this.getClass().getSimpleName() + "_" + plan.getPID() + ".project";

    stopWatch = new StopWatch(4);
  }

  /**
   * This method rewrites an input schema of column-partitioned table because
   * there are no actual field values in data file in a column-partitioned table.
   * So, this method removes partition key columns from the input schema.
   *
   * TODO - This implementation assumes that a fragment is always FileFragment.
   * In the column partitioned table, a path has an important role to
   * indicate partition keys. In this time, it is right. Later, we have to fix it.
   */
  private void rewriteColumnPartitionedTableSchema() throws IOException {
    stopWatch.reset(1);
    PartitionMethodDesc partitionDesc = plan.getTableDesc().getPartitionMethod();
    Schema columnPartitionSchema = SchemaUtil.clone(partitionDesc.getExpressionSchema());
    String qualifier = inSchema.getColumn(0).getQualifier();
    columnPartitionSchema.setQualifier(qualifier);

    // Remove partition key columns from an input schema.
    this.inSchema = plan.getTableDesc().getSchema();

    List<FileFragment> fileFragments = FragmentConvertor.convert(FileFragment.class, fragments);

    // Get a partition key value from a given path
    Tuple partitionRow =
        TupleUtil.buildTupleFromPartitionPath(columnPartitionSchema, fileFragments.get(0).getPath(), false);

    // Targets or search conditions may contain column references.
    // However, actual values absent in tuples. So, Replace all column references by constant datum.
    for (Column column : columnPartitionSchema.toArray()) {
      FieldEval targetExpr = new FieldEval(column);
      Datum datum = targetExpr.eval(columnPartitionSchema, partitionRow);
      ConstEval constExpr = new ConstEval(datum);


      for (int i = 0; i < plan.getTargets().length; i++) {
        Target target = plan.getTargets()[i];

        if (target.getEvalTree().equals(targetExpr)) {
          if (!target.hasAlias()) {
            target.setAlias(target.getEvalTree().getName());
          }
          target.setExpr(constExpr);

        } else {
          EvalTreeUtil.replace(target.getEvalTree(), targetExpr, constExpr);
        }
      }

      if (plan.hasQual()) {
        EvalTreeUtil.replace(plan.getQual(), targetExpr, constExpr);
      }
    }
    this.nanoTimeInit = stopWatch.checkNano(1);
  }

  public void init() throws IOException {
    stopWatch.reset(1);
    Schema projected;

    if (plan.hasTargets()) {
      projected = new Schema();
      Set<Column> columnSet = new HashSet<Column>();

      if (plan.hasQual()) {
        columnSet.addAll(EvalTreeUtil.findUniqueColumns(qual));
      }

      for (Target t : plan.getTargets()) {
        columnSet.addAll(EvalTreeUtil.findUniqueColumns(t.getEvalTree()));
      }

      for (Column column : inSchema.getColumns()) {
        if (columnSet.contains(column)) {
          projected.addColumn(column);
        }
      }
    } else {
      projected = outSchema;
    }

    if (cacheKey != null) {
      TupleCache tupleCache = TupleCache.getInstance();
      if (tupleCache.isBroadcastCacheReady(cacheKey)) {
        openCacheScanner();
      } else {
        if (TupleCache.getInstance().lockBroadcastScan(cacheKey)) {
          scanAndAddCache(projected);
          openCacheScanner();
        } else {
          Object lockMonitor = tupleCache.getLockMonitor();
          synchronized (lockMonitor) {
            try {
              lockMonitor.wait(20 * 1000);
            } catch (InterruptedException e) {
            }
          }
          if (tupleCache.isBroadcastCacheReady(cacheKey)) {
            openCacheScanner();
          } else {
            initScanner(projected);
          }
        }
      }
    } else {
      initScanner(projected);
    }

    super.init();
    this.nanoTimeInit = stopWatch.checkNano(1);
  }

  @Override
  protected void compile() throws CompilationError {
    if (plan.hasQual()) {
      qual = context.getPrecompiledEval(inSchema, qual);
    }
  }

  private void initScanner(Schema projected) throws IOException {
    this.projector = new Projector(context, inSchema, outSchema, plan.getTargets());
    if (fragments != null) {
      if (fragments.length > 1) {
        this.scanner = new MergeScanner(context.getConf(), plan.getPhysicalSchema(), plan.getTableDesc().getMeta(),
            FragmentConvertor.<FileFragment>convert(context.getConf(), plan.getTableDesc().getMeta().getStoreType(),
                fragments), projected
        );
      } else {
        this.scanner = StorageManagerFactory.getStorageManager(
            context.getConf()).getScanner(plan.getTableDesc().getMeta(), plan.getPhysicalSchema(), fragments[0],
            projected);
      }
      scanner.init();
    }
  }

  private void openCacheScanner() throws IOException {
    Scanner cacheScanner = TupleCache.getInstance().openCacheScanner(cacheKey, plan.getPhysicalSchema());
    if (cacheScanner != null) {
      scanner = cacheScanner;
      cacheRead = true;
    }
  }

  private void scanAndAddCache(Schema projected) throws IOException {
    initScanner(projected);

    List<Tuple> broadcastTupleCacheList = new ArrayList<Tuple>();
    while (true) {
      Tuple tuple = next();
      if (tuple != null) {
        broadcastTupleCacheList.add(tuple);
      } else {
        break;
      }
    }

    if (scanner != null) {
      scanner.close();
      scanner = null;
    }

    TupleCache.getInstance().addBroadcastCache(cacheKey, broadcastTupleCacheList);
  }

  private long nanoTimeEval;
  private long nanoTimeProject;

  @Override
  public Tuple next() throws IOException {
    if (fragments == null) {
      return null;
    }

    stopWatch.reset(0);
    try {
      Tuple tuple;
      Tuple outTuple = new VTuple(outColumnNum);

      if (!plan.hasQual()) {
        if ((tuple = scanner.next()) != null) {
          numInTuple++;
          if (cacheRead) {
            numOutTuple++;
            return tuple;
          }
          stopWatch.reset(2);
          projector.eval(tuple, outTuple);
          outTuple.setOffset(tuple.getOffset());
          numOutTuple++;
          nanoTimeProject += stopWatch.checkNano(2);
          return outTuple;
        } else {
          return null;
        }
      } else {
        while ((tuple = scanner.next()) != null) {
          numInTuple++;
          if (cacheRead) {
            numOutTuple++;
            return tuple;
          }
          stopWatch.reset(3);
          Datum evalResult = qual.eval(inSchema, tuple);
          nanoTimeEval += stopWatch.checkNano(3);
          if (evalResult.isTrue()) {
            stopWatch.reset(2);
            projector.eval(tuple, outTuple);
            nanoTimeProject += stopWatch.checkNano(2);
            numOutTuple++;
            return outTuple;
          }
        }
        return null;
      }
    } finally {
      nanoTimeNext += stopWatch.checkNano(0);
    }
  }

  @Override
  public void rescan() throws IOException {
    scanner.reset();
  }

  int pid;

  @Override
  public void close() throws IOException {
    IOUtils.cleanup(null, scanner);
    if (scanner != null) {
      try {
        TableStats stat = scanner.getInputStats();
        if (stat != null) {
          inputStats = (TableStats)(stat.clone());
        }
      } catch (CloneNotSupportedException e) {
        e.printStackTrace();
      }
    }
    if (evalNodeKey != null) {
      putProfileMetrics(pid, evalNodeKey + ".nanoTime", nanoTimeEval);
    }

    putProfileMetrics(pid, projectorEvalNodeKey + ".nanoTime", nanoTimeProject);

    closeProfile(pid);
    scanner = null;
    plan = null;
    qual = null;
    projector = null;
  }

  public String getTableName() {
    return plan.getTableName();
  }

  @Override
  public float getProgress() {
    if (scanner == null) {
      return 1.0f;
    } else {
      return scanner.getProgress();
    }
  }

  @Override
  public TableStats getInputStats() {
    if (scanner != null) {
      return scanner.getInputStats();
    } else {
      return inputStats;
    }
  }

  @Override
  public String toString() {
    if (scanner != null) {
      return "SeqScanExec:" + plan + "," + scanner.getClass().getName();
    } else {
      return "SeqScanExec:" + plan;
    }
  }
}
