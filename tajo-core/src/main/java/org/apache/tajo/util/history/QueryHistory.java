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

public class QueryHistory implements GsonObject, History {
  @Expose
  private String queryId;
  @Expose
  private String queryMaster;
  @Expose
  private int httpPort;
  @Expose
  private List<String[]> sessionVariables;
  @Expose
  private String logicalPlan;
  @Expose
  private String distributedPlan;

  private List<SubQueryHistory> subQueryHistories;

  public String getQueryId() {
    return queryId;
  }

  public void setQueryId(String queryId) {
    this.queryId = queryId;
  }

  public void setQueryMaster(String queryMaster) {
    this.queryMaster = queryMaster;
  }

  public void setSubQueryHistories(List<SubQueryHistory> subQueryHistories) {
    this.subQueryHistories = subQueryHistories;
  }

  public String getQueryMaster() {
    return queryMaster;
  }

  public int getHttpPort() {
    return httpPort;
  }

  public void setHttpPort(int httpPort) {
    this.httpPort = httpPort;
  }

  public List<SubQueryHistory> getSubQueryHistories() {
    return subQueryHistories;
  }

  public List<String[]> getSessionVariables() {
    return sessionVariables;
  }

  public String getLogicalPlan() {
    return logicalPlan;
  }

  public String getDistributedPlan() {
    return distributedPlan;
  }

  public void setSessionVariables(List<String[]> sessionVariables) {
    this.sessionVariables = sessionVariables;
  }

  public void setLogicalPlan(String logicalPlan) {
    this.logicalPlan = logicalPlan;
  }

  public void setDistributedPlan(String distributedPlan) {
    this.distributedPlan = distributedPlan;
  }

  @Override
  public String toJson() {
    return CoreGsonHelper.toJson(this, QueryHistory.class);
  }

  @Override
  public HistoryType getHistoryType() {
    return HistoryType.QUERY;
  }
}
