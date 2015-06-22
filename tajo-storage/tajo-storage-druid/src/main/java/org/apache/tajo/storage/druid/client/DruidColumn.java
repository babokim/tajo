/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.storage.druid.client;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class DruidColumn {
  private String type;
  private int size;
  private int cardinality;
  private String errorMessage;

  @JsonCreator
  public DruidColumn (@JsonProperty("type") String type,
                      @JsonProperty("size") int size,
                      @JsonProperty("cardinality") int cardinality,
                      @JsonProperty("errorMessage") String errorMessage) {
    this.type = type;
    this.size = size;
    this.cardinality = cardinality;
    this.errorMessage = errorMessage;
  }

  @JsonProperty("type")
  public String getType() {
    return type;
  }

  @JsonProperty("size")
  public int getSize() {
    return size;
  }

  @JsonProperty("cardinality")
  public int getCardinality() {
    return cardinality;
  }

  @JsonProperty("errorMessage")
  public String getErrorMessage() {
    return errorMessage;
  }
}
