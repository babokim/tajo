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

package org.apache.tajo.worker;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.common.ProtoObject;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.querymaster.QueryUnit;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType.*;

/**
 * <code>FetchImpl</code> information to indicate the locations of intermediate data.
 */
public class FetchImpl implements ProtoObject<TajoWorkerProtocol.FetchProto>, Cloneable {
  private final static int HTTP_REQUEST_MAXIMUM_LENGTH = 1900;
  private TajoWorkerProtocol.FetchProto.Builder builder = null;

  private QueryUnit.PullHost host;             // The pull server host information
  private TajoWorkerProtocol.ShuffleType type; // hash or range partition method.
  private ExecutionBlockId executionBlockId;   // The executionBlock id
  private int partitionId;                     // The hash partition id
  private String name;                         // The intermediate source name
  private String rangeParams;                  // optional, the http parameters of range partition. (e.g., start=xx&end=yy)
  private boolean hasNext = false;             // optional, if true, has more taskIds

  private List<Integer> taskIds;               // repeated, the task ids
  private List<Integer> attemptIds;            // repeated, the attempt ids

  //In the case of adjust fetch mode single fetch is responsible from partitionId to endPartitionId
  private int endPartitionId = -1;

  public FetchImpl() {
    builder = TajoWorkerProtocol.FetchProto.newBuilder();
    taskIds = new ArrayList<Integer>();
    attemptIds = new ArrayList<Integer>();
  }

  public FetchImpl(TajoWorkerProtocol.FetchProto proto) {
    this(new QueryUnit.PullHost(proto.getHost(), proto.getPort()),
        proto.getType(),
        new ExecutionBlockId(proto.getExecutionBlockId()),
        proto.getPartitionId(),
        proto.getRangeParams(),
        proto.getHasNext(),
        proto.getName(),
        proto.getTaskIdList(),
        proto.getAttemptIdList(),
        proto.getEndPartitionId());
  }

  public FetchImpl(QueryUnit.PullHost host, TajoWorkerProtocol.ShuffleType type, ExecutionBlockId executionBlockId,
                   int partitionId) {
    this(host, type, executionBlockId, partitionId, null, false, null,
        new ArrayList<Integer>(), new ArrayList<Integer>(), -1);
  }

  public FetchImpl(QueryUnit.PullHost host, TajoWorkerProtocol.ShuffleType type, ExecutionBlockId executionBlockId,
                   int partitionId, List<QueryUnit.IntermediateEntry> intermediateEntryList) {
    this(host, type, executionBlockId, partitionId, null, false, null,
        new ArrayList<Integer>(), new ArrayList<Integer>(), -1);
    for (QueryUnit.IntermediateEntry entry : intermediateEntryList){
      addPart(entry.getTaskId(), entry.getAttemptId());
    }
  }

  public FetchImpl(QueryUnit.PullHost host, TajoWorkerProtocol.ShuffleType type, ExecutionBlockId executionBlockId,
                   int partitionId, String rangeParams, boolean hasNext, String name,
                   List<Integer> taskIds, List<Integer> attemptIds,
                   int endPartitionId) {
    this.host = host;
    this.type = type;
    this.executionBlockId = executionBlockId;
    this.partitionId = partitionId;
    this.rangeParams = rangeParams;
    this.hasNext = hasNext;
    this.name = name;
    this.taskIds = taskIds;
    this.attemptIds = attemptIds;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(host, type, executionBlockId,
        partitionId, endPartitionId, name, rangeParams, hasNext, taskIds, attemptIds);
  }

  @Override
  public TajoWorkerProtocol.FetchProto getProto() {
    if (builder == null) {
      builder = TajoWorkerProtocol.FetchProto.newBuilder();
    }
    builder.setHost(host.getHost());
    builder.setPort(host.getPort());
    builder.setType(type);
    builder.setExecutionBlockId(executionBlockId.getProto());
    builder.setPartitionId(partitionId);
    builder.setHasNext(hasNext);
    builder.setName(name);

    if (rangeParams != null && !rangeParams.isEmpty()) {
      builder.setRangeParams(rangeParams);
    }

    Preconditions.checkArgument(taskIds.size() == attemptIds.size());
    builder.addAllTaskId(taskIds);
    builder.addAllAttemptId(attemptIds);
    builder.setEndPartitionId(endPartitionId);
    return builder.build();
  }

  public void addPart(int taskId, int attemptId) {
    this.taskIds.add(taskId);
    this.attemptIds.add(attemptId);
  }

  public QueryUnit.PullHost getPullHost() {
    return this.host;
  }

  public ExecutionBlockId getExecutionBlockId() {
    return executionBlockId;
  }

  public void setExecutionBlockId(ExecutionBlockId executionBlockId) {
    this.executionBlockId = executionBlockId;
  }

  public int getPartitionId() {
    return partitionId;
  }

  public void setPartitionId(int partitionId) {
    this.partitionId = partitionId;
  }

  public String getRangeParams() {
    return rangeParams;
  }

  public void setRangeParams(String rangeParams) {
    this.rangeParams = rangeParams;
  }

  public boolean hasNext() {
    return hasNext;
  }

  public void setHasNext(boolean hasNext) {
    this.hasNext = hasNext;
  }

  public TajoWorkerProtocol.ShuffleType getType() {
    return type;
  }

  public void setType(TajoWorkerProtocol.ShuffleType type) {
    this.type = type;
  }

  /**
   * Get the pull server URIs.
   */
  public List<URI> getURIs(){
    return createFetchURL(this, true);
  }

  /**
   * Get the pull server URIs without repeated parameters.
   */
  public List<URI> getSimpleURIs(){
    return createFetchURL(this, false);
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<Integer> getTaskIds() {
    return taskIds;
  }

  public List<Integer> getAttemptIds() {
    return attemptIds;
  }

  public int getEndPartitionId() {
    return endPartitionId;
  }

  public void setEndPartitionId(int endPartitionId) {
    this.endPartitionId = endPartitionId;
  }

  public FetchImpl clone() throws CloneNotSupportedException {
    FetchImpl newFetchImpl = (FetchImpl) super.clone();

    newFetchImpl.builder = TajoWorkerProtocol.FetchProto.newBuilder();
    newFetchImpl.host = host.clone();
    newFetchImpl.type = type;
    newFetchImpl.executionBlockId = executionBlockId;
    newFetchImpl.partitionId = partitionId;
    newFetchImpl.name = name;
    newFetchImpl.rangeParams = rangeParams;
    newFetchImpl.hasNext = hasNext;
    if (taskIds != null) {
      newFetchImpl.taskIds = Lists.newArrayList(taskIds);
    }
    if (attemptIds != null) {
      newFetchImpl.attemptIds = Lists.newArrayList(attemptIds);
    }
    newFetchImpl.endPartitionId = endPartitionId;
    return newFetchImpl;
  }

  public boolean isPartitionRanged() {
    return getEndPartitionId() >= 0;
  }

  public static List<URI> createFetchURL(FetchImpl fetch, boolean includeParts) {
    String scheme = "http://";

    String partitionParam = String.valueOf(fetch.getPartitionId());
    if (fetch.isPartitionRanged()) {
      partitionParam += "-" + fetch.getEndPartitionId();
    }
    StringBuilder urlPrefix = new StringBuilder(scheme);
    urlPrefix.append(fetch.getPullHost().getHost()).append(":").append(fetch.getPullHost().getPort()).append("/?")
        .append("qid=").append(fetch.getExecutionBlockId().getQueryId().toString())
        .append("&sid=").append(fetch.getExecutionBlockId().getId())
        .append("&p=").append(partitionParam)
        .append("&type=");
    if (fetch.getType() == HASH_SHUFFLE) {
      urlPrefix.append("h");
    } else if (fetch.getType() == RANGE_SHUFFLE) {
      urlPrefix.append("r").append("&").append(fetch.getRangeParams());
    } else if (fetch.getType() == SCATTERED_HASH_SHUFFLE) {
      urlPrefix.append("s");
    }

    List<URI> fetchURLs = new ArrayList<URI>();
    if(includeParts) {
      if (fetch.isPartitionRanged()) {
        urlPrefix.append("&ta=all");
        fetchURLs.add(URI.create(urlPrefix.toString()));
      } else {
        // If the get request is longer than 2000 characters,
        // the long request uri may cause HTTP Status Code - 414 Request-URI Too Long.
        // Refer to http://www.w3.org/Protocols/rfc2616/rfc2616-sec10.html#sec10.4.15
        // The below code transforms a long request to multiple requests.
        List<String> taskIdsParams = new ArrayList<String>();
        StringBuilder taskIdListBuilder = new StringBuilder();
        List<Integer> taskIds = fetch.getTaskIds();
        List<Integer> attemptIds = fetch.getAttemptIds();
        boolean first = true;

        for (int i = 0; i < taskIds.size(); i++) {
          StringBuilder taskAttemptId = new StringBuilder();

          if (!first) { // when comma is added?
            taskAttemptId.append(",");
          } else {
            first = false;
          }

          int taskId = taskIds.get(i);
          int attemptId = attemptIds.get(i);
          taskAttemptId.append(taskId).append("_").append(attemptId);

          if (taskIdListBuilder.length() + taskAttemptId.length()
              > HTTP_REQUEST_MAXIMUM_LENGTH) {
            taskIdsParams.add(taskIdListBuilder.toString());
            taskIdListBuilder = new StringBuilder(taskId + "_" + attemptId);
          } else {
            taskIdListBuilder.append(taskAttemptId);
          }
        }
        // if the url params remain
        if (taskIdListBuilder.length() > 0) {
          taskIdsParams.add(taskIdListBuilder.toString());
        }

        urlPrefix.append("&ta=");
        for (String param : taskIdsParams) {
          fetchURLs.add(URI.create(urlPrefix + param));
        }
      }
    } else {
      fetchURLs.add(URI.create(urlPrefix.toString()));
    }

    return fetchURLs;
  }
}
