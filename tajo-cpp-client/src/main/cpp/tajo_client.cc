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

#include <cassert>
#include <iostream>
#include <sstream>

#include <boost/shared_ptr.hpp>
#include <boost/regex.hpp>

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TTransportUtils.h>
#include <thrift/transport/TTransport.h>
#include <thrift/transport/TSocket.h>

#include "TajoThriftService.h"
#include "tajo_types.h"

#include "tajo_client.h"
#include "tajo_resultset.h"
#include "tajo_thrift_connection.h"

using namespace std;
using namespace boost;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::tajo;
using namespace apache::tajo::thrift;

bool IsQueryRunnning(string state) {
  return (state.compare("QUERY_NEW") == 0) ||
    (state.compare("QUERY_RUNNING") == 0) ||
    (state.compare("QUERY_MASTER_LAUNCHED") == 0) ||
    (state.compare("QUERY_MASTER_INIT") == 0) ||
    (state.compare("QUERY_NOT_ASSIGNED") == 0);
}

TajoThriftConnection* OpenConnection(const char* database, const char* host, int port, char* err, size_t err_len) {
  boost::shared_ptr<TSocket> socket(new TSocket(host, port));

  boost::shared_ptr<TTransport> transport(new TBufferedTransport(socket));
  boost::shared_ptr<TBinaryProtocol> protocol(new TBinaryProtocol(transport));
  boost::shared_ptr<TajoThriftServiceClient> client(new TajoThriftServiceClient(protocol));
  try {
    transport->open();
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, NULL);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Unable to connect to Tajo Thrift Server.", err, err_len, NULL);
  }
  TajoThriftConnection* conn = new TajoThriftConnection(client, transport);

  //TODO set with System User.
  conn->user_id_ = "test";

  conn->base_database_ = string(database);

  return conn;
}

TajoReturn CloseConnection(TajoThriftConnection* conn, char* err, size_t err_len) {
  RETURN_ON_ASSERT(conn == NULL, __FUNCTION__,
    "Tajo connection cannot be NULL.", err, err_len, TAJO_ERROR);
  RETURN_ON_ASSERT(conn->transport_ == NULL, __FUNCTION__,
    "Tajo connection transport cannot be NULL.", err, err_len, TAJO_ERROR);
  try {
    conn->transport_->close();
  } catch (...) {
    //Ignore the exception, we just want to clean up everything...
  }
  delete conn;
  return TAJO_SUCCESS;
}

TajoReturn CreateDatabase(TajoThriftConnection* conn, const char* database_name, char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    conn->client_->createDatabase(conn->session_id_, database_name);
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling createDatabase to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn ExistDatabase(TajoThriftConnection* conn, const char* database_name, int* exists,
  char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    bool exists_result = conn->client_->existDatabase(conn->session_id_, database_name);
    *exists = (exists_result ? 1 : 0);
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling ExistDatabase to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn DropDatabase(TajoThriftConnection* conn, const char* database_name, char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    conn->client_->dropDatabase(conn->session_id_, database_name);
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling DropDatabase to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn GetAllDatabaseNames(TajoThriftConnection* conn, char*** databases,
  size_t* num_databases, char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    vector<string> database_names;
    conn->client_->getAllDatabases(database_names, conn->session_id_);

    *num_databases = database_names.size();
    *databases = new char*[database_names.size()];
    for (int i = 0; i < *num_databases; i++) {
      (*databases)[i] = new char[database_names[i].length() + 1];
      strncpy((*databases)[i], database_names[i].c_str(), database_names[i].length());
    }
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetAllDatabaseNames to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn GetTableList(TajoThriftConnection* conn, const char* database_name, std::vector<std::string>& tables,
  char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    conn->client_->getTableList(tables, conn->session_id_, database_name);
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetTables to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn GetTables(TajoThriftConnection* conn, const char* database_name, const char* table_name,
  TajoResultSet** result_set, char* err, size_t err_len) {

  vector<string> table_names;

  TajoReturn result = GetTableList(conn, database_name, table_names, err, sizeof(err));

  if (result != TAJO_SUCCESS) {
    return result;
  }

  vector<TRowData> rows;
  for (vector<string>::iterator it = table_names.begin(); it != table_names.end(); ++it) {

    if ((table_name != NULL && strlen(table_name) > 0) && strcmp(table_name, it->c_str()) != 0) {
      continue;
    }
    TRowData rowData;

    rowData.columnDatas.push_back(database_name); // TABLE_CAT
    rowData.columnDatas.push_back("public");      // TABLE_SCHEM
    rowData.columnDatas.push_back(*it);           // TABLE_NAME
    rowData.columnDatas.push_back("TABLE");       // TABLE_TYPE
    rowData.columnDatas.push_back("");            // REMARKS
    rowData.columnDatas.push_back("0");           // COL_MAX

    rowData.nullFlags.push_back(false);
    rowData.nullFlags.push_back(false);
    rowData.nullFlags.push_back(false);
    rowData.nullFlags.push_back(false);
    rowData.nullFlags.push_back(true);
    rowData.nullFlags.push_back(true);

    rows.push_back(rowData);
  }

  TQueryResult query_result;
  query_result.rows = rows;

  char const* column_names[] = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "TABLE_TYPE", "REMARKS", "COL_MAX"};

  string column_types[] = {"TEXT", "TEXT", "TEXT", "TEXT", "TEXT", "INT4"};

  for (int i = 0; i < 6; i++) {
    TColumn column;
    column.name = column_names[i];
    column.dataTypeName = column_types[i];
    query_result.schema.columns.push_back(column);
  }
  *result_set = new TajoResultSet(conn, "", DEFAULT_FETCH_SIZE, query_result, true);

  return TAJO_SUCCESS;
}

TajoReturn GetTableDesc(TajoThriftConnection* conn, const char* table_name, TTableDesc& tableDesc,
  char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    conn->client_->getTableDesc(tableDesc, conn->session_id_, table_name);
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetTableDesc to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn GetColumns(TajoThriftConnection* conn, const char* database_name, const char* table_name,
  TajoResultSet** result_set, char* err, size_t err_len) {

  vector<string> table_names;

  TajoReturn status = GetTableList(conn, database_name, table_names, err, err_len);

  if (status != TAJO_SUCCESS) {
    return status;
  }

  char null_flags[] = {false, false, false, false, false
    , false, false, false, false, false
    , false, true, true, true, false
    , false, false, false, false};

  vector<TRowData> rows;

  // TODO use regexp
  //boost::regex table_name_regex(table_name);
  for (vector<string>::iterator it = table_names.begin(); it != table_names.end(); ++it) {
    string each_table_name = *it;
    //if (!boost::regex_match(each_table_name, table_name_regex)) {
    //  continue;
    //}
    if (strcmp(table_name, each_table_name.c_str()) != 0) {
      continue;
    }
    TTableDesc table_desc;
    status = GetTableDesc(conn, each_table_name.c_str(), table_desc, err, err_len);
    if (status != TAJO_SUCCESS) {
      return status;
    }

    vector<TColumn> columns = table_desc.schema.columns;
    int col_pos = 0;
    for (vector<TColumn>::iterator col_it = columns.begin(); col_it != columns.end(); col_it++) {
      TRowData rowData;

      char sql_type_buf [10];
      sprintf(sql_type_buf, "%d", (*col_it).sqlDataType);

      char pos_buf [10];
      sprintf(pos_buf, "%d", col_pos);

      rowData.columnDatas.push_back(database_name);           // TABLE_CAT
      rowData.columnDatas.push_back("public");                // TABLE_SCHEM
      rowData.columnDatas.push_back(each_table_name.c_str()); // TABLE_NAME
      rowData.columnDatas.push_back((*col_it).simpleName);    // COLUMN_NAME
      rowData.columnDatas.push_back(sql_type_buf);            // DATA_TYPE
      rowData.columnDatas.push_back((*col_it).sqlDataTypeName);   // TYPE_NAME
      rowData.columnDatas.push_back("0");                     // COLUMN_SIZE
      rowData.columnDatas.push_back("0");                     // BUFFER_LENGTH
      rowData.columnDatas.push_back("0");                     // DECIMAL_DIGITS
      rowData.columnDatas.push_back("0");                     // NUM_PREC_RADIX
      rowData.columnDatas.push_back("1");                     // NULLABLE
      rowData.columnDatas.push_back("");                      // REMARKS
      rowData.columnDatas.push_back("");                      // COLUMN_DEF
      rowData.columnDatas.push_back("");                      // SQL_DATA_TYPE
      rowData.columnDatas.push_back("");                      // SQL_DATETIME_SUB
      rowData.columnDatas.push_back("0");                     // CHAR_OCTET_LENGTH
      rowData.columnDatas.push_back(pos_buf);                 // ORDINAL_POSITION
      rowData.columnDatas.push_back("YES");                   // IS_NULLABLE
      rowData.columnDatas.push_back("0");                     // COL_MAX

      for (int i = 0; i < 19; i++) {
        rowData.nullFlags.push_back(null_flags[i]);
      }

      rows.push_back(rowData);

      col_pos++;
    }
  }

  TQueryResult query_result;
  query_result.rows = rows;

  char const* column_names[] = {"TABLE_CAT", "TABLE_SCHEM", "TABLE_NAME", "COLUMN_NAME", "DATA_TYPE"
    , "TYPE_NAME", "COLUMN_SIZE", "BUFFER_LENGTH", "DECIMAL_DIGITS", "NUM_PREC_RADIX"
    , "NULLABLE", "REMARKS", "COLUMN_DEF", "SQL_DATA_TYPE", "SQL_DATETIME_SUB"
    , "CHAR_OCTET_LENGTH", "ORDINAL_POSITION", "IS_NULLABLE", "COL_MAX"};

  string column_types[] = {
    "TEXT", "TEXT", "TEXT", "TEXT", "INT4"
    , "TEXT", "INT4", "INT4", "INT4", "INT4"
    , "INT4", "TEXT", "TEXT", "INT4", "INT4"
    , "INT4", "INT4", "TEXT", "INT4"};


  for (int i = 0; i < 19; i++) {
    TColumn column;
    column.name = column_names[i];
    column.dataTypeName = column_types[i];
    query_result.schema.columns.push_back(column);
  }

  *result_set = new TajoResultSet(conn, "", DEFAULT_FETCH_SIZE, query_result, true);

  return TAJO_SUCCESS;
}

TajoReturn DropTable(TajoThriftConnection* conn, const char* table_name, int purge, char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    conn->client_->dropTable(conn->session_id_, table_name, (purge == 1));
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling DropTable to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

// Session API
TajoReturn GetCurrentDatabase(TajoThriftConnection* conn, char* database, char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    string database_param;
    conn->client_->getCurrentDatabase(database_param, conn->session_id_);
    database = new char[database_param.length()];
    strncpy(database, database_param.c_str(), database_param.length());
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetCurrentDatabase to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn SelectDatabase(TajoThriftConnection* conn, const char* database, char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    TServerResponse response;
    conn->client_->selectDatabase(response, conn->session_id_, database);
    if (!response.boolResult) {
      string error_message =
        string("Failed while calling SelectDatabase to Tajo Thrift Server") + response.errorMessage;
      RETURN_FAILURE(__FUNCTION__, error_message.c_str(), err, err_len, TAJO_ERROR);
    }
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling SelectDatabase to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn UpdateSessionVariable(TajoThriftConnection* conn, char *key, char *value, char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    conn->client_->updateSessionVariable(conn->session_id_, key, value);
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling UpdateSessionVariable to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn UnsetSessionVariable(TajoThriftConnection* conn, char *key, char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    conn->client_->unsetSessionVariables(conn->session_id_, key);
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling UnsetSessionVariable to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn GetAllSessionVariables(TajoThriftConnection* conn, TajoSessionVariable** variables, size_t* num_variables,
  char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    map<string, string> variable_map;
    conn->client_->getAllSessionVariables(variable_map, conn->session_id_);

    *num_variables = variable_map.size();
    variables = new TajoSessionVariable*[variable_map.size()];
    int i = 0;
    for (std::map<string, string>::iterator it = variable_map.begin(); it != variable_map.end(); ++it) {
      variables[i] = new TajoSessionVariable;
      variables[i]->key = new char[it->first.length() + 1];
      strncpy(variables[i]->key, it->first.c_str(), it->first.length());
      variables[i]->value = new char[it->second.length() + 1];
      strncpy(variables[i]->value, it->second.c_str(), it->second.length());
    }
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetAllSessionVariables to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn CheckSessionAndGet(TajoThriftConnection* conn, char* err, size_t err_len) {
  if (conn->session_id_.empty()) {

    TServerResponse response;
    conn->client_->createSession(response, conn->user_id_, conn->base_database_);
    conn->session_id_ = response.sessionId;
  }

  return TAJO_SUCCESS;
}

//Query API
TajoReturn RunQuery(TajoThriftConnection* conn, const char *sql, TGetQueryStatusResponse& response,
  char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    conn->client_->submitQuery(response, conn->session_id_, sql, false);
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetAllSessionVariables to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn UpdateQuery(TajoThriftConnection* conn, const char *sql, char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    TServerResponse response;
    conn->client_->updateQuery(response, conn->session_id_, sql);
    if (strcmp(response.resultCode.c_str(), "ERROR") == 0) {
      RETURN_FAILURE(__FUNCTION__, response.errorMessage.c_str(), err, err_len, TAJO_ERROR);
    }
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetAllSessionVariables to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn GetQueryStatus(TajoThriftConnection* conn, const string query_id, TGetQueryStatusResponse& response,
  char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    conn->client_->getQueryStatus(response, conn->session_id_, query_id);
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling getQueryStatus to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn CreateNullResultSet(TajoThriftConnection* conn, const string query_id,
  TajoResultSet** result_set, char *err, size_t err_len) {
  TQueryResult empty_query_result;
  vector<TRowData> empty_row_datas;
  empty_query_result.rows = empty_row_datas;

  *result_set = new TajoResultSet(conn, query_id, DEFAULT_FETCH_SIZE, empty_query_result, true);

  return TAJO_SUCCESS;
}

TajoReturn GetQueryResultSet(TajoThriftConnection* conn, const string query_id,
  const TGetQueryStatusResponse& query_response,
  TajoResultSet**  result_set, char *err, size_t err_len) {
  if (query_response.queryResult.rows.size() > 0) {
    //select 1+1
    TQueryResult query_result = query_response.queryResult;
    *result_set = new TajoResultSet(conn, "", DEFAULT_FETCH_SIZE, query_result, true);
    return TAJO_SUCCESS;
  }

  TGetQueryStatusResponse status;
  //boost::posix_time::millisec sleep_interval(100);
  while(1) {
    TajoReturn status_result = GetQueryStatus(conn, query_id, status, err, err_len);
    if (status_result == TAJO_SUCCESS && !IsQueryRunnning(status.state)) {
      //cout << "status_result:" << status_result << ", state: " << status.state << endl;
      break;
    }
    //boost::this_thread::sleep(sleep_interval);
    sleep(1);
  }

  if (status.state.compare("QUERY_SUCCEEDED") == 0 ) {
    if (status.hasResult) {
      *result_set = new TajoResultSet(conn, query_id, DEFAULT_FETCH_SIZE, status.queryResult, false);
      return (*result_set)->GetNextQueryResult(err, err_len);
    } else {
      CreateNullResultSet(conn, query_id, result_set, err, err_len);
    }
  } else {
    ostringstream err_stream;
    err_stream <<  "Query (" << query_id << ") failed: " << status.state << " cause " << status.errorMessage;
    RETURN_FAILURE(__FUNCTION__, err_stream.str().c_str(), err, err_len, TAJO_ERROR);
  }

  return TAJO_SUCCESS;
}

TajoReturn ExecuteQuery(TajoThriftConnection* conn, const char *sql,
  TajoResultSet** result_set, char* err, size_t err_len) {
  try {
    CheckSessionAndGet(conn, err, err_len);
    TGetQueryStatusResponse response;
    conn->client_->submitQuery(response, conn->session_id_, sql, false);
    GetQueryResultSet(conn, response.queryId, response, result_set, err, err_len);
  } catch (TTransportException& ttx) {
    RETURN_FAILURE(__FUNCTION__, ttx.what(), err, err_len, TAJO_ERROR);
  } catch (...) {
    RETURN_FAILURE(__FUNCTION__,
      "Failed while calling GetAllSessionVariables to Tajo Thrift Server.", err, err_len, TAJO_ERROR);
  }
  return TAJO_SUCCESS;
}

TajoReturn Fetch(TajoResultSet* resultset, char* err, size_t err_len) {
  return resultset->Fetch(err, err_len);
}

TajoReturn CloseResultSet(TajoResultSet* resultset, char* err, size_t err_len) {
  return resultset->Close(err, err_len);
}

// GetData API
TajoReturn GetText(TajoResultSet* resultset, size_t column_index, char *buffer, size_t buffer_len,
  size_t *data_byte_size, int *is_null_value, char *err, size_t err_len) {
  return resultset->GetText(column_index, buffer, buffer_len, data_byte_size, is_null_value, err, err_len);
}

TajoReturn GetInt4(TajoResultSet* resultset, size_t column_index, int *buffer, int *is_null_value,
  char *err, size_t err_len) {
  return resultset->GetInt4(column_index, buffer, is_null_value, err, err_len);
}

TajoReturn GetInt8(TajoResultSet* resultset, size_t column_index, long *buffer, int *is_null_value,
  char *err, size_t err_len) {
  return resultset->GetInt8(column_index, buffer, is_null_value, err, err_len);
}

TajoReturn GetFloat8(TajoResultSet* resultset, size_t column_index, double *buffer, int *is_null_value,
  char *err, size_t err_len) {
  return resultset->GetFloat8(column_index, buffer, is_null_value, err, err_len);
}

struct TajoTypePair {
  const char* type_name;
  const TajoDataType type_value;
};

static const TajoTypePair g_tajo_type_table[] = {
  {"BOOLEAN",   TAJO_BOOLEAN_TYPE},
  {"INT1",      TAJO_INT1_TYPE},
  {"INT2",      TAJO_INT2_TYPE},
  {"INT4",      TAJO_INT4_TYPE},
  {"INT8",      TAJO_INT8_TYPE},
  {"FLOAT4",    TAJO_FLOAT4_TYPE},
  {"FLOAT8",    TAJO_FLOAT8_TYPE},
  {"TEXT",      TAJO_TEXT_TYPE},
  {"DATE",      TAJO_DATE_TYPE},
  {"DATETIME",  TAJO_DATETIME_TYPE},
  {"TIMESTAMP", TAJO_TIMESTAMP_TYPE}
};

TajoDataType LookupTajoDataType(const char* type_name) {
  assert(type_name != NULL);
  for (unsigned int idx = 0; idx < LENGTH(g_tajo_type_table); idx++) {
    if (strcmp(type_name, g_tajo_type_table[idx].type_name) == 0) {
      return g_tajo_type_table[idx].type_value;
    }
  }
  return TAJO_UNKNOWN_TYPE;
}

TajoReturn GetColumnCount(TajoResultSet* resultset, size_t* col_count, char* err, size_t err_len) {
  *col_count = resultset->schema().columns.size();
  return TAJO_SUCCESS;
}

TajoReturn GetTajoColumn(TajoResultSet* resultset, size_t column_index, TajoColumn** tajo_column_ptr,
  char *err, size_t err_len) {
  TColumn column = resultset->schema().columns[column_index - 1];

  if (column_index < 1 || column_index > resultset->schema().columns.size()) {
    ostringstream err_stream;
    err_stream << "Wrong column index (" << column_index << ")" ;
    RETURN_FAILURE(__FUNCTION__, err_stream.str().c_str(), err, err_len, TAJO_ERROR);
  }

  TajoDataType tajo_type = LookupTajoDataType(column.dataTypeName.c_str());
  if (tajo_type == TAJO_UNKNOWN_TYPE) {
    ostringstream err_stream;
    err_stream << "Unknown data type (" << column.dataTypeName << ")";
    RETURN_FAILURE(__FUNCTION__, err_stream.str().c_str(), err, err_len, TAJO_ERROR);
  }

  TajoColumn* tajo_column = new TajoColumn;

  tajo_column->column_name = new char[column.simpleName.length() + 1];
  strncpy(tajo_column->column_name, column.simpleName.c_str(), column.simpleName.length());

  tajo_column->type_name = new char[column.dataTypeName.length() + 1];
  strncpy(tajo_column->type_name, column.dataTypeName.c_str(), column.dataTypeName.length());

  tajo_column->type = tajo_type;

  *tajo_column_ptr = tajo_column;
  return TAJO_SUCCESS;
}

size_t GetMaxDisplaySize(TajoDataType type) {
  switch (type) {
    case TAJO_BOOLEAN_TYPE:
      return 1;

    case TAJO_INT1_TYPE:
      return 4;

    case TAJO_INT2_TYPE:
      return 6;

    case TAJO_INT4_TYPE:
      return 11;

    case TAJO_INT8_TYPE:
      return 20;

    case TAJO_FLOAT4_TYPE:
      return 16;

    case TAJO_FLOAT8_TYPE:
      return 24;

    case TAJO_TEXT_TYPE:
      return MAX_DISPLAY_SIZE;

    case TAJO_DATE_TYPE:
      return MAX_DISPLAY_SIZE;

    case TAJO_DATETIME_TYPE:
      return MAX_DISPLAY_SIZE;

    case TAJO_TIMESTAMP_TYPE:
      return MAX_DISPLAY_SIZE;

    default:
      return MAX_DISPLAY_SIZE;
  }
}

size_t GetFieldByteSize(TajoDataType type) {
  switch (type) {
    case TAJO_BOOLEAN_TYPE:
      return 1;

    case TAJO_INT1_TYPE:
      return 1;

    case TAJO_INT2_TYPE:
      return 2;

    case TAJO_INT4_TYPE:
      return 4;

    case TAJO_INT8_TYPE:
      return 8;

    case TAJO_FLOAT4_TYPE:
      return 4;

    case TAJO_FLOAT8_TYPE:
      return 8;

    case TAJO_TEXT_TYPE:
      return MAX_BYTE_LENGTH;

    case TAJO_DATE_TYPE:
      return MAX_BYTE_LENGTH;

    case TAJO_DATETIME_TYPE:
      return MAX_BYTE_LENGTH;

    case TAJO_TIMESTAMP_TYPE:
      return MAX_BYTE_LENGTH;

    default:
      return MAX_BYTE_LENGTH;
  }
}