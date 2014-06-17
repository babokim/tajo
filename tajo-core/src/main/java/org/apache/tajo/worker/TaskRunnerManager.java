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

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.proto.YarnProtos;
import org.apache.tajo.ExecutionBlockId;
import org.apache.tajo.QueryUnitAttemptId;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.master.rm.TajoWorkerContainerId;
import org.apache.tajo.worker.event.TaskRunnerEvent;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

public class TaskRunnerManager extends CompositeService implements EventHandler<TaskRunnerEvent> {
  private static final Log LOG = LogFactory.getLog(TaskRunnerManager.class);

  private final Map<ExecutionBlockId, TaskRunnerContext> taskRunnerContextMap = Maps.newConcurrentMap();
  private final Map<String, TaskRunner> taskRunnerMap = Maps.newConcurrentMap();
  private final Map<String, TaskRunnerHistory> taskRunnerHistoryMap = Maps.newConcurrentMap();
  private TajoWorker.WorkerContext workerContext;
  private TajoConf tajoConf;
  private AtomicBoolean stop = new AtomicBoolean(false);
  private FinishedTaskCleanThread finishedTaskCleanThread;
  private Dispatcher dispatcher;
  private ExecutorService taskExecutor;
  private ExecutorService fetcherExecutor;

  public TaskRunnerManager(TajoWorker.WorkerContext workerContext, Dispatcher dispatcher) {
    super(TaskRunnerManager.class.getName());

    this.workerContext = workerContext;
    this.dispatcher = dispatcher;
  }

  public TajoWorker.WorkerContext getWorkerContext() {
    return workerContext;
  }

  @Override
  public void init(Configuration conf) {
    Preconditions.checkArgument(conf instanceof TajoConf);
    tajoConf = (TajoConf)conf;
    dispatcher.register(TaskRunnerEvent.EventType.class, this);

    taskExecutor = Executors.newFixedThreadPool(tajoConf.getIntVar(TajoConf.ConfVars.WORKER_EXECUTION_MAX_SLOTS));
    fetcherExecutor = Executors.newFixedThreadPool(tajoConf.getIntVar(TajoConf.ConfVars.SHUFFLE_FETCHER_PARALLEL_EXECUTION_MAX_NUM));
    super.init(tajoConf);
  }


  @Override
  public void start() {
    finishedTaskCleanThread = new FinishedTaskCleanThread();
    finishedTaskCleanThread.start();
    super.start();
  }

  @Override
  public void stop() {
    if(stop.getAndSet(true)) {
      return;
    }

    super.stop();

    synchronized(taskRunnerMap) {
      for(TaskRunner eachTaskRunner: taskRunnerMap.values()) {
        if(!eachTaskRunner.isStopped()) {
          eachTaskRunner.stop();
        }
      }
    }

    taskExecutor.shutdownNow();
    fetcherExecutor.shutdownNow();

    if(finishedTaskCleanThread != null) {
      finishedTaskCleanThread.interrupted();
    }

    if(workerContext.isYarnContainerMode()) {
      workerContext.stopWorker(true);
    }
  }

  public void stopTask(String id) {
    LOG.info("Stop Task:" + id);
    synchronized(taskRunnerMap) {
      TaskRunner runner = taskRunnerMap.remove(id);
      runner.getContext().releaseShuffleChannelFactory();
      taskRunnerContextMap.remove(runner.getContext().getExecutionBlockId());
    }
    if(workerContext.isYarnContainerMode()) {
      stop();
    }
  }

  public Collection<TaskRunner> getTaskRunners() {
    synchronized(taskRunnerMap) {
      return Collections.unmodifiableCollection(taskRunnerMap.values());
    }
  }

  public Collection<TaskRunnerHistory> getExecutionBlockHistories() {
    synchronized(taskRunnerHistoryMap) {
      return Collections.unmodifiableCollection(taskRunnerHistoryMap.values());
    }
  }

  public TaskRunnerHistory getExcutionBlockHistoryByTaskRunnerId(String taskRunnerId) {
    synchronized(taskRunnerHistoryMap) {
      return taskRunnerHistoryMap.get(taskRunnerId);
    }
  }

  public TaskRunner getTaskRunner(String taskRunnerId) {
    synchronized(taskRunnerMap) {
      return taskRunnerMap.get(taskRunnerId);
    }
  }

  public Task getTaskByQueryUnitAttemptId(QueryUnitAttemptId quAttemptId) {
    synchronized(taskRunnerMap) {
      for (TaskRunner eachTaskRunner: taskRunnerMap.values()) {
        Task task = eachTaskRunner.getContext().getTask(quAttemptId);
        if (task != null) return task;
      }
    }
    return null;
  }

  public TaskHistory getTaskHistoryByQueryUnitAttemptId(QueryUnitAttemptId quAttemptId) {
    synchronized (taskRunnerHistoryMap) {
      for (TaskRunnerHistory history : taskRunnerHistoryMap.values()) {
        TaskHistory taskHistory = history.getTaskHistory(quAttemptId);
        if (taskHistory != null) return taskHistory;
      }
    }

    return null;
  }

  public int getNumTasks() {
    synchronized(taskRunnerMap) {
      return taskRunnerMap.size();
    }
  }

  @Override
  public void handle(TaskRunnerEvent event) {
    LOG.info("======================== Processing " + event.getExecutionBlockId() + " of type " + event.getType());
    if (event.getType() == TaskRunnerEvent.EventType.TASK_START) {

      try {

        TajoConf systemConf = new TajoConf(tajoConf);
        for (YarnProtos.ContainerIdProto containerIdProto : event.getContainers()) {

          TajoWorkerContainerId containerId = new TajoWorkerContainerId(containerIdProto);
          TaskRunnerContext context = taskRunnerContextMap.get(event.getExecutionBlockId());
          if(context == null){
            context = new TaskRunnerContext(this, event.getExecutionBlockId());
            taskRunnerContextMap.put(event.getExecutionBlockId(), context);
          }

          //TODO change the TaskRunner to service
          TaskRunner taskRunner = new TaskRunner(context, systemConf,
              containerId, event.getQueryMasterNode());

          LOG.info("Start TaskRunner:" + taskRunner.getId());
          synchronized (taskRunnerMap) {
            taskRunnerMap.put(taskRunner.getId(), taskRunner);
          }

          synchronized (taskRunnerHistoryMap) {
            taskRunnerHistoryMap.put(taskRunner.getId(), taskRunner.getHistory());
          }

          taskRunner.init(systemConf);
          taskRunner.start();
          Future future = taskExecutor.submit(taskRunner);
          LOG.info("started task : " + future.isDone());
        }
      } catch (Exception e) {
        LOG.error(e.getMessage(), e);
        throw new RuntimeException(e.getMessage(), e);
      }
    }
  }

  public EventHandler getEventHandler(){
    return dispatcher.getEventHandler();
  }

  public void startTask(final String[] params) {
    //TODO change to use event dispatcher
//    Thread t = new Thread() {
//      public void run() {
//        try {
//          TajoConf systemConf = new TajoConf(tajoConf);
//          TaskRunner taskRunner = new TaskRunner(TaskRunnerManager.this, systemConf, params);
//          LOG.info("Start TaskRunner:" + taskRunner.getId());
//          synchronized(taskRunnerMap) {
//            taskRunnerMap.put(taskRunner.getId(), taskRunner);
//          }
//
//          synchronized (taskRunnerHistoryMap){
//            taskRunnerHistoryMap.put(taskRunner.getId(), taskRunner.getContext().getExcutionBlockHistory());
//          }
//
//          taskRunner.init(systemConf);
//          taskRunner.start();
//        } catch (Exception e) {
//          LOG.error(e.getMessage(), e);
//          throw new RuntimeException(e.getMessage(), e);
//        }
//      }
//    };
//
//    t.start();
  }

  public ExecutorService getFetcherExecutor() {
    return fetcherExecutor;
  }

  public TajoConf getTajoConf() {
    return tajoConf;
  }

  class FinishedTaskCleanThread extends Thread {
    //TODO if history size is large, the historyMap should remove immediately
    public void run() {
      int expireIntervalTime = tajoConf.getIntVar(TajoConf.ConfVars.WORKER_HISTORY_EXPIRE_PERIOD);
      LOG.info("FinishedQueryMasterTaskCleanThread started: expire interval minutes = " + expireIntervalTime);
      while(!stop.get()) {
        try {
          Thread.sleep(60 * 1000 * 60);   // hourly check
        } catch (InterruptedException e) {
          break;
        }
        try {
          long expireTime = System.currentTimeMillis() - expireIntervalTime * 60 * 1000;
          cleanExpiredFinishedQueryMasterTask(expireTime);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }

    private void cleanExpiredFinishedQueryMasterTask(long expireTime) {
      synchronized(taskRunnerHistoryMap) {
        List<String> expiredIds = new ArrayList<String>();
        for(Map.Entry<String, TaskRunnerHistory> entry: taskRunnerHistoryMap.entrySet()) {
          if(entry.getValue().getStartTime() > expireTime) {
            expiredIds.add(entry.getKey());
          }
        }

        for(String eachId: expiredIds) {
          taskRunnerHistoryMap.remove(eachId);
        }
      }
    }
  }
}
