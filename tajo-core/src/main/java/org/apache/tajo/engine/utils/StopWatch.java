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

package org.apache.tajo.engine.utils;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class StopWatch {
  Map<String, AtomicLong> currentMillis;
  Map<String, AtomicLong> currentNano;

  private boolean enable;
  public StopWatch(boolean enable) {
    this.enable = enable;
    if (enable) {
      currentMillis = new HashMap<String, AtomicLong>();
      currentNano = new HashMap<String, AtomicLong>();
    }
  }
  public void reset(String key) {
    if (!enable) {
      return;
    }
    AtomicLong millis = currentMillis.get(key);
    if (millis == null) {
      currentMillis.put(key, new AtomicLong(System.currentTimeMillis()));
    } else {
      millis.set(System.currentTimeMillis());
    }

    AtomicLong nano = currentNano.get(key);
    if (nano == null) {
      currentNano.put(key, new AtomicLong(System.nanoTime()));
    } else {
      nano.set(System.nanoTime());
    }
  }

  public long checkNano(String key) {
    if (!enable) {
      return 0L;
    }
    long nano = System.nanoTime();
    try {
      long lastValue = currentNano.get(key).getAndSet(nano);
      return nano - lastValue;
    } catch (Throwable t) {
      reset(key);
      long lastValue = currentNano.get(key).getAndSet(nano);
      return nano - lastValue;
    }
  }

  public long checkMillis(String key) {
    if (!enable) {
      return 0L;
    }
    long millis = System.nanoTime();
    try {
      long lastValue = currentMillis.get(key).getAndSet(millis);
      return millis - lastValue;
    } catch (Throwable t) {
      reset(key);
      long lastValue = currentMillis.get(key).getAndSet(millis);
      return millis -lastValue;
    }
  }

  public void clearAll() {
    if (!enable) {
      return;
    }
    currentMillis.clear();
    currentNano.clear();
  }
}
