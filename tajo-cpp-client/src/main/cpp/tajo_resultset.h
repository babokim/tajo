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

#ifndef TAJO_RESULTSET_H_
#define TAJO_RESULTSET_H_

#include <iostream>
#include "tajo_types.h"
#include "tajo_constants.h"
#include "tajo_thrift_connection.h"

typedef enum TajoReturn TajoReturn;

class TajoResultSet {
public:
  /* Constructor for Query result */
  TajoResultSet(TajoThriftConnection *conn, std::string query_id, size_t fetch_size,
    apache::tajo::thrift::TQueryResult query_result, bool init_rows);

  TajoReturn Fetch(char* err, size_t err_len);
  TajoReturn GetText(size_t column_index, char *buffer, size_t buffer_len, size_t *data_byte_size,
    int *is_null_value, char *err, size_t err_len);
  TajoReturn GetInt4(size_t column_index, int *buffer, int *is_null_value, char *err, size_t err_len);
  TajoReturn GetInt8(size_t column_index, long *buffer, int *is_null_value, char *err, size_t err_len);
  TajoReturn GetFloat4(size_t column_index, double *buffer, int *is_null_value, char *err, size_t err_len);
  TajoReturn GetFloat8(size_t column_index, double *buffer, int *is_null_value, char *err, size_t err_len);
  TajoReturn Close(char *err, size_t err_len);
  apache::tajo::thrift::TSchema schema() { return schema_; }
  TajoReturn GetNextQueryResult(char *err, size_t err_len);

  ~TajoResultSet();

private:
  TajoThriftConnection* conn_;
  std::string query_id_;
  apache::tajo::thrift::TQueryResult query_result_;
  apache::tajo::thrift::TSchema schema_;
  std::vector<apache::tajo::thrift::TRowData> rows_;
  int current_row_index_;
  size_t fetch_size_;
  bool no_more_data_;
  bool closed_;

  TajoReturn CloseQuery(char* err, size_t err_len);

};  // TajoResultSet

#endif  // TAJO_RESULTSET_H_