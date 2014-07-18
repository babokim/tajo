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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryId;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.TajoIdProtos;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.ipc.TajoMasterProtocol;
import org.apache.tajo.ipc.TajoWorkerProtocol;
import org.apache.tajo.master.TaskRunnerGroupEvent;
import org.apache.tajo.master.TaskRunnerLauncher;
import org.apache.tajo.master.cluster.WorkerConnectionInfo;
import org.apache.tajo.master.event.ContainerAllocationEvent;
import org.apache.tajo.master.event.ContainerAllocatorEventType;
import org.apache.tajo.master.event.SubQueryContainerAllocationEvent;
import org.apache.tajo.master.event.WorkerResourceRequestEvent;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.querymaster.SubQuery;
import org.apache.tajo.master.querymaster.SubQueryState;
import org.apache.tajo.rpc.CallFuture;
import org.apache.tajo.rpc.NettyClientBase;
import org.apache.tajo.rpc.NullCallback;
import org.apache.tajo.rpc.RpcConnectionPool;
import org.apache.tajo.rpc.protocolrecords.PrimitiveProtos;
import org.apache.tajo.scheduler.MultiQueueFiFoScheduler;
import org.apache.tajo.scheduler.Scheduler;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.tajo.ipc.TajoMasterProtocol.WorkerResourceAllocationResponse;

public class TajoResourceAllocator extends AbstractResourceAllocator {
  private static final Log LOG = LogFactory.getLog(TajoResourceAllocator.class);

  private TajoConf tajoConf;
  private QueryMasterTask.QueryMasterTaskContext queryTaskContext;
  private final ExecutorService executorService;
  private final ExecutorService releaseService;

  /**
   * A key is a worker unique id, and a value is allocated worker resources.
   */
  private ConcurrentMap<Integer, LinkedList<TajoMasterProtocol.AllocatedWorkerResourceProto>> allocatedResourceMap =
      Maps.newConcurrentMap();
  /** allocated resources and not released  */
  private AtomicInteger allocatedSize = new AtomicInteger(0); //TODO handle from scheduler


  private WorkerResourceAllocator allocatorThread;

  public TajoResourceAllocator(QueryMasterTask.QueryMasterTaskContext queryTaskContext) {
    this.queryTaskContext = queryTaskContext;
    this.executorService = Executors.newFixedThreadPool(
        queryTaskContext.getConf().getIntVar(TajoConf.ConfVars.YARN_RM_TASKRUNNER_LAUNCH_PARALLEL_NUM));

    this.releaseService = Executors.newFixedThreadPool(
        queryTaskContext.getConf().getIntVar(TajoConf.ConfVars.YARN_RM_TASKRUNNER_LAUNCH_PARALLEL_NUM));
  }

  @Override
  public void init(Configuration conf) {
    tajoConf = (TajoConf) conf;

    queryTaskContext.getDispatcher().register(TaskRunnerGroupEvent.EventType.class, new TajoTaskRunnerLauncher());
    queryTaskContext.getDispatcher().register(ContainerAllocatorEventType.class, new TajoWorkerAllocationHandler());
    queryTaskContext.getDispatcher().register(WorkerResourceRequestEvent.EventType.class, new WorkerResourceHandler());

    super.init(conf);
  }

  @Override
  protected void serviceStop() throws Exception {
    allocatorThread.shutdown();

    for (int workerId : allocatedResourceMap.keySet()){
      try{
        releaseWorkerResource(queryTaskContext.getQueryId(), workerId);
      } catch (Throwable t){
        LOG.fatal(t.getMessage(), t);
      }
    }
    allocatedResourceMap.clear();
    allocatedSize.set(0);
    executorService.shutdownNow();
    releaseService.shutdownNow();
    super.serviceStop();
    LOG.info("Tajo Resource Allocator stopped");
  }

  @Override
  public void start() {
    super.start();
    allocatorThread = new WorkerResourceAllocator(this);
    allocatorThread.start();
  }

  @Override
  public int calculateNumRequestContainers(TajoWorker.WorkerContext workerContext,
                                           int numTasks,
                                           int memoryMBPerTask,
                                           boolean isLeaf) {
    TajoMasterProtocol.ClusterResourceSummary clusterResource = workerContext.getClusterResource();
    int clusterSlots;
    if(isLeaf){
      int diskSlots = clusterResource == null ? 0 : (int)(clusterResource.getTotalDiskSlots() / 0.5f);
      int memSlots = clusterResource == null ? 0 : clusterResource.getTotalMemoryMB() / memoryMBPerTask;
      clusterSlots = Math.min(diskSlots, memSlots);
    } else {
      clusterSlots = clusterResource == null ? 0 : clusterResource.getTotalMemoryMB() / memoryMBPerTask;
    }
    clusterSlots = Math.max(1, clusterSlots - 1); // reserve query master slot
    LOG.info("CalculateNumberRequestContainer - Number of Tasks=" + numTasks +
        ", Number of Cluster Slots=" + clusterSlots);
    return Math.min(numTasks, clusterSlots);
  }

  class TajoTaskRunnerLauncher implements TaskRunnerLauncher {
    @Override
    public void handle(TaskRunnerGroupEvent event) {
      if (event.getType() == TaskRunnerGroupEvent.EventType.CONTAINER_REMOTE_LAUNCH) {
        //launchTaskRunners(event.getExecutionBlockId(), event.getAllocatedResources());
      } else if (event.getType() == TaskRunnerGroupEvent.EventType.CONTAINER_REMOTE_CLEANUP) {
        allocatorThread.stopWorkerResourceAllocator(event.getExecutionBlockId());
        stopExecutionBlock(event.getExecutionBlockId());
      }
    }
  }

  private void launchTaskRunners(final ExecutionBlockId executionBlockId, final Map<Integer, Integer> allocatedResources) {
    // Query in standby mode doesn't need launch Worker.
    // But, Assign ExecutionBlock to assigned tajo worker
    List<Integer> workerIds = Lists.newArrayList(allocatedResources.keySet());
    Collections.shuffle(workerIds);
    for (final int workerId : workerIds) {
      executorService.submit(new Runnable() {
        @Override
        public void run() {
         executeExecutionBlock(executionBlockId, workerId, allocatedResources.get(workerId));
        }
      });
    }
  }

  public void stopExecutionBlock(final ExecutionBlockId executionBlockId) {
    for (final int workerId : workerInfoMap.keySet()) {
      releaseService.submit(new Runnable() {
        @Override
        public void run() {
          releaseWorkerResource(queryTaskContext.getQueryId(), workerId);
          stopExecutionBlock(executionBlockId, workerId);
        }
      });
    }
  }



  /**
   * It sends a release rpc request to the resource manager.
   *
   * @param workerId a worker id.
   * @param executionBlockId
   * @param resources resource size
   */
  @Override
  public void releaseWorkerResource(final ExecutionBlockId executionBlockId, final int workerId, final int resources) {
    if (allocatedResourceMap.containsKey(workerId)) {
      final List<TajoMasterProtocol.AllocatedWorkerResourceProto> requestList;
      if (allocatedResourceMap.get(workerId).size() == resources) {
        requestList = allocatedResourceMap.remove(workerId);
      } else {
        requestList = new ArrayList<TajoMasterProtocol.AllocatedWorkerResourceProto>();
        LinkedList<TajoMasterProtocol.AllocatedWorkerResourceProto> allocatedWorkerResources = allocatedResourceMap.get(workerId);
        for (int i = 0; i < resources; i++) {
          TajoMasterProtocol.AllocatedWorkerResourceProto allocatedWorkerResourceProto = allocatedWorkerResources.poll();
          if (allocatedWorkerResourceProto == null) {
            break;
          }
          requestList.add(allocatedWorkerResourceProto);
        }
      }

      if(requestList.size() == 0){
        allocatedResourceMap.remove(workerId);
        return;
      }

      releaseService.submit(new Runnable() {
        @Override
        public void run() {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Release Worker: "+ workerId+", EBId : "+executionBlockId+", Resources: " + requestList);
          }
          releaseWorkerResourceRequest(executionBlockId.getQueryId(), requestList);
        }
      });
    }
  }

  private void releaseWorkerResource(final QueryId queryId, final int workerId) {
    if (allocatedResourceMap.containsKey(workerId)) {
      final List<TajoMasterProtocol.AllocatedWorkerResourceProto> requestList = allocatedResourceMap.remove(workerId);
      if(requestList.size() == 0){
        return;
      }

      releaseService.submit(new Runnable() {
        @Override
        public void run() {
          if (LOG.isDebugEnabled()) {
            LOG.debug("Release Worker: "+ workerId+", QueryId : "+queryId+", Resources: " + requestList);
          }
          releaseWorkerResourceRequest(queryId, requestList);
        }
      });
    }
  }

  /**
   * It sends a kill RPC request to a corresponding worker.
   *
   * @param workerId a worker id.
   * @param taskAttemptId The TaskAttemptId to be killed.
   */
  @Override
  public void killTaskAttempt(int workerId, QueryUnitAttemptId taskAttemptId) {
    NettyClientBase tajoWorkerRpc = null;
    try {
      WorkerConnectionInfo connectionInfo = getWorkerConnectionInfo(workerId);
      InetSocketAddress addr = new InetSocketAddress(connectionInfo.getHost(), connectionInfo.getPeerRpcPort());
      tajoWorkerRpc = RpcConnectionPool.getPool(tajoConf).getConnection(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();
      tajoWorkerRpcClient.killTaskAttempt(null, taskAttemptId.getProto(), NullCallback.get());

      releaseWorkerResource(taskAttemptId.getQueryUnitId().getExecutionBlockId(), workerId, 1);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      RpcConnectionPool.getPool(tajoConf).releaseConnection(tajoWorkerRpc);
    }
  }

  private void executeExecutionBlock(ExecutionBlockId executionBlockId, int workerId, int launchTasks) {
    NettyClientBase tajoWorkerRpc = null;
    try {
      WorkerConnectionInfo connectionInfo = getWorkerConnectionInfo(workerId);
      if(connectionInfo == null) return;

      InetSocketAddress addr = new InetSocketAddress(connectionInfo.getHost(), connectionInfo.getPeerRpcPort());
      tajoWorkerRpc = RpcConnectionPool.getPool(tajoConf).getConnection(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();

      WorkerConnectionInfo queryMaster = queryTaskContext.getQueryMasterContext().getWorkerContext().getConnectionInfo();
      TajoWorkerProtocol.RunExecutionBlockRequestProto request =
          TajoWorkerProtocol.RunExecutionBlockRequestProto.newBuilder()
              .setExecutionBlockId(executionBlockId.getProto())
              .setQueryMaster(queryMaster.getProto())
              .setTasks(launchTasks)
              .setQueryOutputPath(queryTaskContext.getStagingDir().toString())
              .build();

      tajoWorkerRpcClient.executeExecutionBlock(null, request, NullCallback.get());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      RpcConnectionPool.getPool(tajoConf).releaseConnection(tajoWorkerRpc);
    }
  }

  private void stopExecutionBlock(ExecutionBlockId executionBlockId, int workerId) {
    NettyClientBase tajoWorkerRpc = null;
    try {
      WorkerConnectionInfo connectionInfo = removeWorker(workerId);

      InetSocketAddress addr = new InetSocketAddress(connectionInfo.getHost(), connectionInfo.getPeerRpcPort());
      tajoWorkerRpc = RpcConnectionPool.getPool(tajoConf).getConnection(addr, TajoWorkerProtocol.class, true);
      TajoWorkerProtocol.TajoWorkerProtocolService tajoWorkerRpcClient = tajoWorkerRpc.getStub();

      tajoWorkerRpcClient.stopExecutionBlock(null, executionBlockId.getProto(), NullCallback.get());
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      RpcConnectionPool.getPool(tajoConf).releaseConnection(tajoWorkerRpc);
    }
  }

  private void releaseWorkerResourceRequest(QueryId queryId,
                                      List<TajoMasterProtocol.AllocatedWorkerResourceProto> resources) {
    if (resources == null || resources.size() == 0) return;

    allocatedSize.getAndAdd(-resources.size());
    RpcConnectionPool connPool = RpcConnectionPool.getPool(queryTaskContext.getConf());
    NettyClientBase tmClient = null;
    try {
      tmClient = connPool.getConnection(queryTaskContext.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
          TajoMasterProtocol.class, true);
      TajoMasterProtocol.TajoMasterProtocolService masterClientService = tmClient.getStub();
      masterClientService.releaseWorkerResource(null,
          TajoMasterProtocol.WorkerResourceReleaseProto.newBuilder()
              .setQueryId(queryId.getProto())
              .addAllResources(resources)
              .build(),
          NullCallback.get()
      );
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
    } finally {
      connPool.releaseConnection(tmClient);
    }

    if(allocatedSize.get() < workerInfoMap.size()) {
      synchronized (allocatorThread) {
        allocatorThread.notifyAll();
      }
    }
  }

  class WorkerResourceHandler implements EventHandler<WorkerResourceRequestEvent> {
    @Override
    public void handle(WorkerResourceRequestEvent event) {
      if (event.getType() == WorkerResourceRequestEvent.EventType.CONTAINER_RELEASE) {
        releaseWorkerResource(event.getExecutionBlockId(), event.getWorkerId(), 1);
      }
    }
  }

  class TajoWorkerAllocationHandler implements EventHandler<ContainerAllocationEvent> {

    @Override
    public void handle(ContainerAllocationEvent event) {
      allocatorThread.startWorkerResourceAllocator(event);
    }
  }

  private TajoMasterProtocol.WorkerResourcesRequestProto createWorkerResourcesRequest(TajoIdProtos.QueryIdProto queryIdProto,
                                                                                      int wokerResource,
                                                                                      TajoMasterProtocol.ResourceRequestPriority requestPriority,
                                                                                      List<Integer> workerIds) {
    //TODO consider task's resource usage pattern
    int requiredMemoryMB = tajoConf.getIntVar(TajoConf.ConfVars.TASK_DEFAULT_MEMORY);
    float requiredDiskSlots = tajoConf.getFloatVar(TajoConf.ConfVars.TASK_DEFAULT_DISK);

    return TajoMasterProtocol.WorkerResourcesRequestProto.newBuilder()
        .setQueryId(queryIdProto)
        .setMinMemoryMBPerContainer(requiredMemoryMB)
        .setMaxMemoryMBPerContainer(requiredMemoryMB)
        .setNumContainers(wokerResource)
        .setResourceRequestPriority(requestPriority)
        .setMinDiskSlotPerContainer(requiredDiskSlots)
        .setMaxDiskSlotPerContainer(requiredDiskSlots)
        .addAllWorkerId(workerIds)
        .build();
  }

  private WorkerResourceAllocationResponse reserveWokerResources(ExecutionBlockId executionBlockId,
                                                                 int required,
                                                                 boolean isLeaf,
                                                                 List<Integer> workerIds) {
    TajoMasterProtocol.ResourceRequestPriority priority =
        isLeaf ? TajoMasterProtocol.ResourceRequestPriority.DISK : TajoMasterProtocol.ResourceRequestPriority.MEMORY;

    CallFuture<WorkerResourceAllocationResponse> callBack =
        new CallFuture<WorkerResourceAllocationResponse>();

    RpcConnectionPool connPool = RpcConnectionPool.getPool(queryTaskContext.getConf());
    NettyClientBase tmClient = null;
    try {
      tmClient = connPool.getConnection(
          queryTaskContext.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
          TajoMasterProtocol.class, true);
      TajoMasterProtocol.TajoMasterProtocolService masterClientService = tmClient.getStub();
      masterClientService.allocateWorkerResources(
          null,
          createWorkerResourcesRequest(executionBlockId.getQueryId().getProto(), required, priority, workerIds),
          callBack);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return null;
    } finally {
      connPool.releaseConnection(tmClient);
    }

    WorkerResourceAllocationResponse response = null;
    while (!isInState(STATE.STOPPED)) {
      try {
        response = callBack.get(3, TimeUnit.SECONDS);
        break;
      } catch (InterruptedException e) {
        if (isInState(STATE.STOPPED)) {
          return null;
        }
      } catch (TimeoutException e) {
        LOG.info("No available worker resource for " + queryTaskContext.getQueryId() +
            ", allocated resources : " + allocatedSize.get());

        continue;
      }
    }
    return response;
  }

  private int getRunningQueries(String queueName) {

    int runningSize = -1;
    CallFuture<PrimitiveProtos.IntProto> callBack = new CallFuture<PrimitiveProtos.IntProto>();
    RpcConnectionPool connPool = RpcConnectionPool.getPool(queryTaskContext.getConf());
    NettyClientBase tmClient = null;
    try {
      tmClient = connPool.getConnection(
          queryTaskContext.getQueryMasterContext().getWorkerContext().getTajoMasterAddress(),
          TajoMasterProtocol.class, true);
      TajoMasterProtocol.TajoMasterProtocolService masterClientService = tmClient.getStub();
      masterClientService.getRunningQueries(callBack.getController(),
          PrimitiveProtos.StringProto.newBuilder().setValue(queueName).build(),
          callBack);
    } catch (Exception e) {
      LOG.error(e.getMessage(), e);
      return runningSize;
    } finally {
      connPool.releaseConnection(tmClient);
    }

    while (!isInState(STATE.STOPPED)) {
      try {
        PrimitiveProtos.IntProto response = callBack.get(3, TimeUnit.SECONDS);
        runningSize = response.getValue();
        return runningSize;
      } catch (InterruptedException e) {
        if (isInState(STATE.STOPPED)) {
          return runningSize;
        }
      } catch (TimeoutException e) {
        if(callBack.getController().failed()){
          return runningSize;
        }
        continue;
      }
    }
    return runningSize;
  }

  private List<Integer> getWorkerIds(Collection<String> hosts){
    List<Integer> workerIds = Lists.newArrayList();
    if(hosts.isEmpty()) return workerIds;

    List<TajoMasterProtocol.WorkerResourceProto> workers =
        queryTaskContext.getQueryMasterContext().getQueryMaster().getAllWorker();
    for (TajoMasterProtocol.WorkerResourceProto worker : workers) {
      if(hosts.contains(worker.getConnectionInfo().getHost())){
        workerIds.add(worker.getConnectionInfo().getId());
      }
    }
    return workerIds;
  }

  class WorkerResourceAllocator extends Thread {
    final int delay = 100;
    final int updateInterval = 1000;
    private AtomicBoolean stop = new AtomicBoolean(false);
    final TajoResourceAllocator allocator;
    final Map<String, MultiQueueFiFoScheduler.QueueProperty> queuePropertyMap;
    final BlockingDeque<WorkerResourceRequest> queue =
        new LinkedBlockingDeque<WorkerResourceRequest>();

    class WorkerResourceRequest {
      AtomicBoolean isFirst = new AtomicBoolean(true);
      AtomicBoolean stop = new AtomicBoolean();
      ContainerAllocationEvent event;
      long updatedTime;
      MultiQueueFiFoScheduler.QueueProperty queueProperty;
      int runningInQueue = 0;
      List<Integer> workerIds;

      public WorkerResourceRequest(ContainerAllocationEvent event, List<Integer> workerIds,
                                   MultiQueueFiFoScheduler.QueueProperty queueProperty) {
        this.event = event;
        this.workerIds = workerIds;
        this.queueProperty = queueProperty;
        this.updatedTime = System.currentTimeMillis();
        this.runningInQueue = allocator.getRunningQueries(queueProperty.getQueueName());
      }
    }

    public WorkerResourceAllocator(TajoResourceAllocator allocator) {
      this.allocator = allocator;
      this.queuePropertyMap = Maps.newHashMap();
      List<MultiQueueFiFoScheduler.QueueProperty> queueProperties = MultiQueueFiFoScheduler.loadQueueProperty(tajoConf);
      for (MultiQueueFiFoScheduler.QueueProperty queueProperty : queueProperties) {
        queuePropertyMap.put(queueProperty.getQueueName(), queueProperty);
      }
    }

    public void startWorkerResourceAllocator(ContainerAllocationEvent event) {
      try {
        LOG.info("Start allocation. required containers(" + event.getRequiredNum() + ") executionBlockId : " + event.getExecutionBlockId());

        for (int workerId : allocatedResourceMap.keySet()) {
          if (allocatedResourceMap.containsKey(workerId)) {
            try {
              releaseWorkerResource(queryTaskContext.getQueryId(), workerId);
            } catch (Throwable t) {
              LOG.fatal(t.getMessage(), t);
            }
          }
        }

        List<Integer> workerIds;
        if(event.isLeafQuery()){
          Set<String> hosts = allocator.queryTaskContext.getSubQuery(event.getExecutionBlockId()).getTaskScheduler().getLeafTaskHosts();
          workerIds = getWorkerIds(hosts);
        } else {
          workerIds = Lists.newArrayList();
        }

        MultiQueueFiFoScheduler.QueueProperty queueProperty =
            queuePropertyMap.get(queryTaskContext.getSession().getVariable(Scheduler.QUERY_QUEUE_KEY, Scheduler.DEFAULT_QUEUE_NAME));

        queue.put(new WorkerResourceRequest(event, workerIds, queueProperty));
      } catch (InterruptedException e) {
        if (!stop.get()) {
          LOG.warn("ContainerAllocator thread interrupted");
        }
      }
    }

    public void stopWorkerResourceAllocator(ExecutionBlockId executionBlockId) {
      if (queue.size() > 0) {
        Iterator<WorkerResourceRequest> iterator = queue.iterator();
        while (iterator.hasNext()){
          WorkerResourceRequest request = iterator.next();
          if(request.event.getExecutionBlockId().equals(executionBlockId)){
            request.stop.set(true);
            iterator.remove();
            LOG.warn("Container allocator force stopped. executionBlockId : " + request.event.getExecutionBlockId());
          }
        }
      }

      if (allocatorThread != null) {
        synchronized (allocatorThread) {
          allocatorThread.notifyAll();
        }
      }
    }

    public synchronized void shutdown() {
      if (stop.getAndSet(true)) {
        return;
      }

      if (queue.size() > 0) {
        Iterator<WorkerResourceRequest> iterator = queue.iterator();
        while (iterator.hasNext()){
          WorkerResourceRequest request = iterator.next();
          request.stop.set(true);
        }
      }

      if (allocatorThread != null) {
        allocatorThread.interrupt();
      }
    }

    @Override
    public void run() {
      while (!stop.get() && !Thread.currentThread().isInterrupted()) {
        WorkerResourceRequest request;
        try {
          request = queue.take();
        } catch (InterruptedException ie) {
          if (!stop.get()) {
            LOG.warn("ContainerAllocator thread interrupted");
          }
          break;
        }
        ExecutionBlockId executionBlockId = request.event.getExecutionBlockId();
        SubQueryState state = queryTaskContext.getSubQuery(executionBlockId).getState();

        /* for scheduler */

        int resources = request.event.getRequiredNum();
        if (request.queueProperty != null && request.queueProperty.getMaxCapacity() > 0) {
          if(request.updatedTime + updateInterval < System.currentTimeMillis()){
            // update the master scheduler status
            int runningSize = allocator.getRunningQueries(request.queueProperty.getQueueName());
            request.runningInQueue = runningSize > 0 ? runningSize : 1;
          }
          int demandSize = (int) Math.floor(request.queueProperty.getMaxCapacity() / request.runningInQueue);
          resources = Math.min(request.event.getRequiredNum(), demandSize);
          request.updatedTime = System.currentTimeMillis();
        }

        try {
          if (!request.stop.get() && SubQuery.isRunningState(state)) {
            queue.addFirst(request);
            if(LOG.isDebugEnabled()){
              LOG.debug("Retry to allocate containers executionBlockId : " + request.event.getExecutionBlockId());
            }
            int remainingTask = allocator.queryTaskContext.getSubQuery(executionBlockId).getTaskScheduler().remainingScheduledObjectNum();

            if (remainingTask <= 0) {
              // in order to reallocate, if a QueryUnitAttempt was failure
              LOG.debug("All Allocated. executionBlockId : " + request.event.getExecutionBlockId());
            } else {
              int availableSize = resources - allocatedSize.get();
              if(LOG.isDebugEnabled()){
                LOG.debug(String.format("requiredNum: %d, allocatedSize: %d, remainingTask: %d", resources, allocatedSize.get(), remainingTask));
              }
              resources = Math.min(remainingTask * 2, resources); // for tail tasks
              int determinedResources  = Math.min(resources, availableSize);
              allocateContainers(request, determinedResources);
            }

            if(!request.stop.get()){
              synchronized (allocatorThread) {
                allocatorThread.wait(delay);
              }
            }
          } else {
            LOG.warn("ExecutionBlock is not running state : " + state + ", " + executionBlockId);
          }
        } catch (InterruptedException e) {
          if (!stop.get()) {
            LOG.warn("ContainerAllocator thread interrupted");
          }
          break;
        }
      }
      LOG.info("ContainerAllocator Stopped");
    }

    private void allocateContainers(WorkerResourceRequest request, int resources) {
      ExecutionBlockId executionBlockId = request.event.getExecutionBlockId();

      if(LOG.isDebugEnabled()){
        LOG.debug("Try to allocate containers executionBlockId : " + executionBlockId + "," + resources);
      }
      if(resources <= 0) return;

      List<Integer> workerIds;
      if(request.event.isLeafQuery() && request.isFirst.get()){
        workerIds = request.workerIds;
      } else {
        workerIds = Lists.newArrayList();
      }
      WorkerResourceAllocationResponse response =
          reserveWokerResources(executionBlockId, resources, request.event.isLeafQuery(), workerIds);
      if (response != null) {
        List<TajoMasterProtocol.AllocatedWorkerResourceProto> allocatedResources = response.getAllocatedWorkerResourceList();
        allocatedSize.addAndGet(allocatedResources.size());

        Map<Integer, Integer> tasksLaunchMap = Maps.newHashMap();
        for (TajoMasterProtocol.AllocatedWorkerResourceProto eachAllocatedResource : allocatedResources) {
          WorkerConnectionInfo connectionInfo = new WorkerConnectionInfo(eachAllocatedResource.getWorker().getConnectionInfo());
          int workerId = connectionInfo.getId();
          addWorkerConnectionInfo(connectionInfo);

          allocatedResourceMap.putIfAbsent(workerId, new LinkedList<TajoMasterProtocol.AllocatedWorkerResourceProto>());
          allocatedResourceMap.get(workerId).add(eachAllocatedResource);

          if(!tasksLaunchMap.containsKey(workerId)){
            tasksLaunchMap.put(workerId, 0);
          }
          tasksLaunchMap.put(workerId, tasksLaunchMap.get(workerId) + 1);
        }

        if (!request.stop.get() && allocatedResources.size() > 0) {
          LOG.info("Reserved worker resources : " + allocatedResources.size()
              + ", EBId : " + executionBlockId);
          LOG.debug("SubQueryContainerAllocationEvent fire:" + executionBlockId);

          if (LOG.isDebugEnabled()) {
            LOG.debug("SubQueryContainerAllocationEvent fire:" + executionBlockId);
          }
          launchTaskRunners(executionBlockId, tasksLaunchMap);

          if(request.isFirst.getAndSet(false)){
            queryTaskContext.getEventHandler().handle(new SubQueryContainerAllocationEvent(executionBlockId, tasksLaunchMap));
          }
        }
      }
    }
  }
}
