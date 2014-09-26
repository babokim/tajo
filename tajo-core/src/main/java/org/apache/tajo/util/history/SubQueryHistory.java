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

package org.apache.tajo.util.history;

import com.google.gson.annotations.Expose;
import org.apache.tajo.engine.json.CoreGsonHelper;
import org.apache.tajo.json.GsonObject;

import java.util.List;

public class SubQueryHistory implements GsonObject {
  @Expose
  private String executionBlockId;
  @Expose
  private String state;
  @Expose private long startTime;
  @Expose private long finishTime;
  @Expose private int succeededObjectCount;
  @Expose private int totalScheduledObjectsCount;

  @Expose private long totalInputBytes;
  @Expose private long totalReadBytes;
  @Expose private long totalReadRows;
  @Expose private long totalWriteBytes;
  @Expose private long totalWriteRows;
  @Expose private int numShuffles;
  @Expose private float progress;

  @Expose private String plan;

  @Expose private List<QueryUnitHistory> queryUnits;

  public String getExecutionBlockId() {
    return executionBlockId;
  }

  public void setExecutionBlockId(String executionBlockId) {
    this.executionBlockId = executionBlockId;
  }

  public String getState() {
    return state;
  }

  public void setState(String state) {
    this.state = state;
  }

  public long getStartTime() {
    return startTime;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public long getFinishTime() {
    return finishTime;
  }

  public void setFinishTime(long finishTime) {
    this.finishTime = finishTime;
  }

  public int getSucceededObjectCount() {
    return succeededObjectCount;
  }

  public void setSucceededObjectCount(int succeededObjectCount) {
    this.succeededObjectCount = succeededObjectCount;
  }

  public int getTotalScheduledObjectsCount() {
    return totalScheduledObjectsCount;
  }

  public void setTotalScheduledObjectsCount(int totalScheduledObjectsCount) {
    this.totalScheduledObjectsCount = totalScheduledObjectsCount;
  }

  public long getTotalInputBytes() {
    return totalInputBytes;
  }

  public void setTotalInputBytes(long totalInputBytes) {
    this.totalInputBytes = totalInputBytes;
  }

  public long getTotalReadBytes() {
    return totalReadBytes;
  }

  public void setTotalReadBytes(long totalReadBytes) {
    this.totalReadBytes = totalReadBytes;
  }

  public long getTotalReadRows() {
    return totalReadRows;
  }

  public void setTotalReadRows(long totalReadRows) {
    this.totalReadRows = totalReadRows;
  }

  public long getTotalWriteBytes() {
    return totalWriteBytes;
  }

  public void setTotalWriteBytes(long totalWriteBytes) {
    this.totalWriteBytes = totalWriteBytes;
  }

  public long getTotalWriteRows() {
    return totalWriteRows;
  }

  public void setTotalWriteRows(long totalWriteRows) {
    this.totalWriteRows = totalWriteRows;
  }

  public int getNumShuffles() {
    return numShuffles;
  }

  public void setNumShuffles(int numShuffles) {
    this.numShuffles = numShuffles;
  }

  public float getProgress() {
    return progress;
  }

  public void setProgress(float progress) {
    this.progress = progress;
  }

  public String getPlan() {
    return plan;
  }

  public void setPlan(String plan) {
    this.plan = plan;
  }

  public List<QueryUnitHistory> getQueryUnits() {
    return queryUnits;
  }

  public void setQueryUnits(List<QueryUnitHistory> queryUnits) {
    this.queryUnits = queryUnits;
  }

  @Override
  public String toJson() {
    return CoreGsonHelper.toJson(this, SubQueryHistory.class);
  }
}
