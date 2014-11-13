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

/**
 * 
 */
package org.apache.tajo.plan.logical;

import com.google.gson.annotations.Expose;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.plan.PlanString;
import org.apache.tajo.plan.Target;
import org.apache.tajo.plan.util.PlannerUtil;
import org.apache.tajo.util.TUtil;

public class EvalExprArrayNode extends LogicalNode implements Projectable {
  @Expose private EvalExprNode[] evalNodes;

  public EvalExprArrayNode(int pid) {
    super(pid, NodeType.EXPRS_ARRAY);
  }

  @Override
  public boolean hasTargets() {
    return true;
  }

  @Override
  public void setTargets(Target[] targets) {
    //setOutSchema(PlannerUtil.targetToSchema(targets));
  }

  @Override
  public Target[] getTargets() {
    return evalNodes[0].getTargets();
  }

  @Override
  public String toString() {
    return "EvalExprArrayNode (" + TUtil.arrayToString(evalNodes) + ")";
  }

  public boolean equals(Object object) {
    if (object instanceof EvalExprArrayNode) {
      EvalExprArrayNode other = (EvalExprArrayNode) object;
      return TUtil.checkEquals(this.evalNodes, other.evalNodes);
    } else {
      return false;
    }
  }
  
  @Override
  public void preOrder(LogicalNodeVisitor visitor) {
    // nothing
  }

  @Override
  public void postOrder(LogicalNodeVisitor visitor) {
    // nothing
  }

  @Override
  public PlanString getPlanString() {
    return null;
  }

  public EvalExprNode[] getEvalNodes() {
    return evalNodes;
  }

  public void setEvalNodes(EvalExprNode[] evalNodes) {
    this.evalNodes = evalNodes;
    if (evalNodes.length > 0) {
      Schema schema = PlannerUtil.targetToSchema(evalNodes[0].getTargets());
      setInSchema(schema);
      setOutSchema(schema);
    }
  }
}
