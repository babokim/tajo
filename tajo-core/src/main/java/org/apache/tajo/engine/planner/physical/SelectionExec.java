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

import org.apache.tajo.engine.eval.EvalNode;
import org.apache.tajo.engine.planner.logical.SelectionNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.util.StopWatch;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

public class SelectionExec extends UnaryPhysicalExec  {
  private final EvalNode qual;

  public SelectionExec(TaskAttemptContext context,
                       SelectionNode plan,
                       PhysicalExec child) {
    super(context, plan.getInSchema(), plan.getOutSchema(), child);
    this.qual = plan.getQual();
    stopWatch = new StopWatch(2);
  }

  String profileKey = getClass().getSimpleName() + ".next";

  @Override
  public Tuple next() throws IOException {
    Tuple tuple;

    stopWatch.reset(0);
    try {
      while ((tuple = child.next()) != null) {
        numInTuple++;
        if (qual.eval(inSchema, tuple).isTrue()) {
          numOutTuple++;
          return tuple;
        }
      }

      return null;
    } finally {
      nanoTimeNext += stopWatch.checkNano(0);
    }
  }

  @Override
  public void close() {
    closeProfile();
  }
}
