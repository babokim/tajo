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

#include "tajo_resultset.h"

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TTransport.h>
#include <thrift/transport/TSocket.h>

#include "TajoThriftService.h"
#include "tajo_types.h"
#include "tajo_client.h"

using namespace std;
using namespace apache::tajo;
using namespace apache::tajo::thrift;
using namespace apache::thrift::transport;

TajoResultSet::TajoResultSet(TajoThriftConnection *conn, string query_id, size_t fetch_size,
  TQueryResult query_result, bool init_rows)
  : conn_(conn), query_id_(query_id), query_result_(query_result), fetch_size_(fetch_size), closed_(false) {
  if (init_rows) {
    rows_ = query_result.rows;
    if (rows_.size() == 0) {
      no_more_data_ = true;
    } else {
      current_row_index_ = -1;
      no_more_data_ = false;

      schema_ = query_result_.tableDesc.schema;
      if (schema_.columns.size() == 0) {
        schema_ = query_result_.schema;
      }
    }
  }
}

TajoResultSet::~TajoResultSet() {

}

TajoReturn TajoResultSet::Fetch(char *err, size_t err_len) {
  if (no_more_data_ || closed_) {
    return TAJO_NO_MORE_DATA;
  }
  current_row_index_++;
  if (rows_.size() == 0 || current_row_index_ >= rows_.size()) {
    if (query_id_.size() > 0) {
      //Fetch next rows from ThriftServer
      TajoReturn status = GetNextQueryResult(err, err_len);
      if (status == TAJO_ERROR) {
        return status;
      }
      if (no_more_data_) {
        return TAJO_NO_MORE_DATA;
      } else {
        current_row_index_ = 0;
      }
    } else {
      no_more_data_ = true;
      return TAJO_NO_MORE_DATA;
    }
  }

  return TAJO_SUCCESS_WITH_MORE_DATA;

}

TajoReturn TajoResultSet::GetText(size_t column_index, char *buffer, size_t buffer_len, size_t *data_byte_size,
  int *is_null_value, char *err, size_t err_len) {
  if (closed_) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetText cause already closed.", err, err_len, TAJO_ERROR);
  }
  if (no_more_data_) {
    return TAJO_NO_MORE_DATA;
  }
  TRowData current_row_ = rows_[current_row_index_];
  if (current_row_.nullFlags[column_index - 1]) {
    *is_null_value = 1;
    buffer[0] = '\0';
    *data_byte_size = 0;
  } else {
    *is_null_value = 0;
    string column_data = current_row_.columnDatas[column_index - 1];

    std::strncpy(buffer, column_data.c_str(), column_data.length());
    *data_byte_size = column_data.length();
  }
  return TAJO_SUCCESS;
}

TajoReturn TajoResultSet::GetInt4(size_t column_index, int *buffer, int *is_null_value,
  char *err, size_t err_len) {
  if (closed_) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetText cause already closed.", err, err_len, TAJO_ERROR);
  }
  if (no_more_data_) {
    return TAJO_NO_MORE_DATA;
  }

  TRowData current_row_ = rows_[current_row_index_];
  if (current_row_.nullFlags[column_index - 1]) {
    *is_null_value = 1;
    *buffer = 0;
  } else {
    *is_null_value = 0;
    string column_data = current_row_.columnDatas[column_index - 1];
    *buffer = atoi (column_data.c_str());
  }

  return TAJO_SUCCESS;
}

TajoReturn TajoResultSet::GetInt8(size_t column_index, long *buffer, int *is_null_value,
  char *err, size_t err_len) {
  if (closed_) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetText cause already closed.", err, err_len, TAJO_ERROR);
  }

  TRowData current_row_ = rows_[current_row_index_];
  if (current_row_.nullFlags[column_index - 1]) {
    *is_null_value = 1;
    *buffer = 0;
  } else {
    *is_null_value = 0;
    string column_data = current_row_.columnDatas[column_index - 1];
    *buffer = atol (column_data.c_str());
  }

  return TAJO_SUCCESS;
}

TajoReturn TajoResultSet::GetFloat4(size_t column_index, double *buffer, int *is_null_value,
  char *err, size_t err_len) {
  if (closed_) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetText cause already closed.", err, err_len, TAJO_ERROR);
  }
  if (no_more_data_) {
    return TAJO_NO_MORE_DATA;
  }

  TRowData current_row_ = rows_[current_row_index_];
  if (current_row_.nullFlags[column_index - 1]) {
    *is_null_value = 1;
    *buffer = 0;
  } else {
    *is_null_value = 0;
    string column_data = current_row_.columnDatas[column_index - 1];
    *buffer = atof (column_data.c_str());
  }

  return TAJO_SUCCESS;
}

TajoReturn TajoResultSet::GetFloat8(size_t column_index, double *buffer, int *is_null_value,
  char *err, size_t err_len) {
  if (closed_) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetText cause already closed.", err, err_len, TAJO_ERROR);
  }
  if (no_more_data_) {
    return TAJO_NO_MORE_DATA;
  }

  TRowData current_row_ = rows_[current_row_index_];
  if (current_row_.nullFlags[column_index - 1]) {
    *is_null_value = 1;
    *buffer = 0;
  } else {
    *is_null_value = 0;
    string column_data = current_row_.columnDatas[column_index - 1];
    *buffer = atof (column_data.c_str());
  }

  return TAJO_SUCCESS;
}

TajoReturn TajoResultSet::Close(char *err, size_t err_len) {
  closed_ = true;
  if (query_id_.size() > 0) {
    return CloseQuery(err, err_len);
  }
  return TAJO_SUCCESS;
}

TajoReturn TajoResultSet::CloseQuery(char* err, size_t err_len) {
  TServerResponse response;
  try {
    conn_->client_->closeQuery(response, conn_->session_id_, query_id_.c_str());
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling closeQuery to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn TajoResultSet::GetNextQueryResult(char *err, size_t err_len) {
  try {
    TQueryResult query_result_param;
    conn_->client_->getQueryResult(query_result_param, conn_->session_id_, query_id_, fetch_size_);
    query_result_ = query_result_param;
    if (schema_.columns.size() == 0) {
      schema_ = query_result_.tableDesc.schema;
      if (schema_.columns.size() == 0) {
        schema_ = query_result_.schema;
      }
    }
    rows_ = query_result_.rows;
    if (rows_.size() == 0) {
      no_more_data_ = true;
    } else {
      no_more_data_ = false;
    }

    current_row_index_ = -1;
  } catch (TTransportException &ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling getQueryResult to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}