/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.engine.planner.global;

import org.apache.tajo.ExecutionBlockId;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A distributed execution plan (DEP) is a direct acyclic graph (DAG) of ExecutionBlocks.
 * This class is a pointer to an ExecutionBlock that the query engine should execute.
 * For each call of nextBlock(), it retrieves a next ExecutionBlock in a postfix order.
 */
public class ExecutionBlockCursor {
  private MasterPlan masterPlan;
  private ArrayList<ExecutionBlock> orderedBlocks = new ArrayList<ExecutionBlock>();
  private int cursor = 0;

  public ExecutionBlockCursor(MasterPlan plan) {
    this(plan, false);
  }

  public ExecutionBlockCursor(MasterPlan plan, boolean executionOrder) {
    this.masterPlan = plan;
    if (executionOrder) {
      buildExecutionOrder(plan.getRoot());
    } else {
      buildOrder(plan.getRoot());
    }
  }

  public int size() {
    return orderedBlocks.size();
  }

  // Add all execution blocks in a depth first and postfix order
  private void buildOrder(ExecutionBlock current) {
    Stack<ExecutionBlock> stack = new Stack<ExecutionBlock>();
    if (!masterPlan.isLeaf(current.getId())) {
      for (ExecutionBlock execBlock : masterPlan.getChilds(current)) {
        if (!masterPlan.isLeaf(execBlock)) {
          buildOrder(execBlock);
        } else {
          stack.push(execBlock);
        }
      }
      for (ExecutionBlock execBlock : stack) {
        buildOrder(execBlock);
      }
    }
    orderedBlocks.add(current);
  }

  List<BuildOrderItem> executionOrderedBlocks = new ArrayList<BuildOrderItem>();
  List<BuildOrderItem> notOrderedSiblingBlocks = new ArrayList<BuildOrderItem>();
  Map<ExecutionBlockId, AtomicInteger> orderRequiredChildCountMap = new HashMap<ExecutionBlockId, AtomicInteger>();

  /**
   *
   * @param current
   */
  private void buildExecutionOrder(ExecutionBlock current) {
    preExecutionOrder(new BuildOrderItem(null, current));

    for (BuildOrderItem eachItem: executionOrderedBlocks) {
      if (masterPlan.isLeaf(eachItem.eb.getId())) {
        orderedBlocks.add(eachItem.eb);
        orderRequiredChildCountMap.get(eachItem.parentEB.getId()).decrementAndGet();
      } else {
        if (eachItem.allSiblingsOrdered()) {
          for (BuildOrderItem eachSiblingItem: notOrderedSiblingBlocks) {
            orderedBlocks.add(eachSiblingItem.eb);
          }
          orderedBlocks.add(eachItem.eb);
          notOrderedSiblingBlocks.clear();
        } else {
          notOrderedSiblingBlocks.add(eachItem);
        }
      }
    }
  }

  private void preExecutionOrder(BuildOrderItem current) {
    Stack<BuildOrderItem> stack = new Stack<BuildOrderItem>();
    if (!masterPlan.isLeaf(current.eb.getId())) {
      List<ExecutionBlock> children = masterPlan.getChilds(current.eb);
      orderRequiredChildCountMap.put(current.eb.getId(), new AtomicInteger(children.size()));
      for (ExecutionBlock execBlock : children) {
        BuildOrderItem item = new BuildOrderItem(current.eb, execBlock);
        item.setSiblings(children);
        if (!masterPlan.isLeaf(execBlock)) {
          preExecutionOrder(item);
        } else {
          stack.push(item);
        }
      }
      for (BuildOrderItem eachItem : stack) {
        preExecutionOrder(eachItem);
      }
    }
    executionOrderedBlocks.add(current);
  }

  class BuildOrderItem {
    ExecutionBlock eb;
    ExecutionBlock parentEB;
    List<ExecutionBlockId> siblings = new ArrayList<ExecutionBlockId>();

    BuildOrderItem(ExecutionBlock parentEB, ExecutionBlock eb) {
      this.parentEB = parentEB;
      this.eb = eb;
    }

    public void setSiblings(List<ExecutionBlock> siblings) {
      for (ExecutionBlock eachEB: siblings) {
        if (eachEB.getId().equals(eb.getId())) {
          continue;
        }

        this.siblings.add(eachEB.getId());
      }
    }

    public boolean allSiblingsOrdered() {
      for (ExecutionBlockId eachSibling: siblings) {
        if (orderRequiredChildCountMap.get(eachSibling) != null &&
            orderRequiredChildCountMap.get(eachSibling).get() > 0) {
          return false;
        }
      }
      return true;
    }

    @Override
    public String toString() {
      return eb.toString();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof BuildOrderItem)) {
        return false;
      }
      return eb.equals(((BuildOrderItem) obj).eb);
    }
  }

  public boolean hasNext() {
    return cursor < orderedBlocks.size();
  }

  public ExecutionBlock nextBlock() {
    return orderedBlocks.get(cursor++);
  }

  public ExecutionBlock peek() {
    return orderedBlocks.get(cursor);
  }

  public ExecutionBlock peek(int skip) {
    return  orderedBlocks.get(cursor + skip);
  }

  public void reset() {
    cursor = 0;
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < orderedBlocks.size(); i++) {
      if (i == (cursor == 0 ? 0 : cursor - 1)) {
        sb.append("(").append(orderedBlocks.get(i).getId().getId()).append(")");
      } else {
        sb.append(orderedBlocks.get(i).getId().getId());
      }

      if (i < orderedBlocks.size() - 1) {
        sb.append(",");
      }
    }

    return sb.toString();
  }
}
