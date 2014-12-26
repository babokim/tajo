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

#include <iostream>
#include <vector>
#include <cassert>

#include "tajo_client.h"
#include "tajo_types.h"
#include "tajo_resultset.h"
#include "tajo_thrift_connection.h"

using namespace std;
using namespace apache::tajo;
using namespace apache::tajo::thrift;

int main() {
  cout << "Tajo C/C++ Client TestCase" << endl;

  char err[128];
  const char* database = "default";
  const char* table_name = "table1";
  const char* thrift_server = "127.0.0.1";

  TajoThriftConnection *conn = OpenConnection(database, thrift_server, 26700, err, sizeof(err));

  // Database API
  const char *test_database = "test_database";
  int exists;
  TajoReturn call_status = ExistDatabase(conn, test_database, &exists, err, sizeof(err));
  assert(call_status == TAJO_SUCCESS);
  if (exists != 1) {
    call_status = CreateDatabase(conn, test_database, err, sizeof(err));
    assert(call_status == TAJO_SUCCESS);
  }

  char** databases;
  size_t num_elements;
  call_status = GetAllDatabaseNames(conn, &databases, &num_elements, err, sizeof(err));
  assert(call_status == TAJO_SUCCESS);
  assert(num_elements > 0);
  cout << "\n===> GetAllDatabaseNames <===" << endl;
  for (int i = 0; i < num_elements; i++) {
    cout << databases[i] << endl;
  }
  for (int i = 0; i < num_elements; i++) {
    free(databases[i]);
  }
  free(databases);

  call_status = ExistDatabase(conn, test_database, &exists, err, sizeof(err));
  assert(call_status == TAJO_SUCCESS);
  cout << "\n===> ExistDatabase <===" << endl;
  //Should be true
  cout << database << " exists: " << (exists == 1 ? "true" : "false") << endl;

  call_status = DropDatabase(conn, test_database, err, sizeof(err));
  assert(call_status == TAJO_SUCCESS);

  call_status = ExistDatabase(conn, test_database, &exists, err, sizeof(err));
  assert(call_status == TAJO_SUCCESS);
  cout << "\n===> ExistDatabase after drop <===" << endl;
  //Should be false
  cout << database << " exists: " << (exists == 1 ? "true" : "false") << endl;

  //Table API
  char col_data[120];
  size_t col_data_len;
  int is_null_value;

  cout << "\n===> GetTables <===" << endl;
  TajoResultSet* table_result_set;
  call_status = GetTables(conn, database, NULL, &table_result_set, err, sizeof(err));
  assert(call_status == TAJO_SUCCESS);
  int num_columns = table_result_set->schema().columns.size();
  assert(num_columns == 6);
  for (int i = 0; i < num_columns; i++) {
    cout << table_result_set->schema().columns[i].name << ", ";
  }
  cout << endl;
  cout << "-----------------------------------------------" << endl;

  while (1) {
    TajoReturn result = table_result_set->Fetch(err, sizeof(err));
    if (result == TAJO_NO_MORE_DATA) {
      break;
    }

    for (int i = 0; i < num_columns; i++) {
      table_result_set->GetText(i + 1, col_data, sizeof(col_data), &col_data_len, &is_null_value, err, sizeof(err));
      cout << string(col_data, 0, col_data_len) << ", ";
    }
    cout << endl;
  }
  table_result_set->Close(err, sizeof(err));

  cout << "\n===> GetColumns <===" << endl;
  TajoResultSet* column_resultset;
  call_status = GetColumns(conn, database, table_name, &column_resultset, err, sizeof(err));
  assert(call_status == TAJO_SUCCESS);

  size_t column_count;
  GetColumnCount(column_resultset, &column_count, err, sizeof(err));
  for (int i = 0; i < column_count; i++) {
    cout << column_resultset->schema().columns[i].name << ", ";
  }
  cout << endl;
  cout << "-----------------------------------------------" << endl;

  while (1) {
    TajoReturn result = column_resultset->Fetch(err, sizeof(err));
    if (result == TAJO_NO_MORE_DATA) {
      break;
    }

    for (int i = 0; i < column_count; i++) {
      column_resultset->GetText(i + 1, col_data, sizeof(col_data), &col_data_len, &is_null_value, err, sizeof(err));
      cout << string(col_data, 0, col_data_len) << ", ";
    }
    cout << endl;
  }
  column_resultset->Close(err, sizeof(err));
  free(column_resultset);

  // Testing ODBC Interface emulating
  const char* sql = "select * from table2 where id > 0";
  cout << "\n===> ExecuteQuery: Basic query <===" << endl;
  TajoResultSet* resultset;
  call_status = ExecuteQuery(conn, sql, &resultset, err, sizeof(err));
  assert(call_status == TAJO_SUCCESS);

  GetColumnCount(resultset, &column_count, err, sizeof(err));
  assert(column_count == 4);

  int num_rows = 0;
  while (1) {
    TajoReturn result = resultset->Fetch(err, sizeof(err));
    if (result == TAJO_ERROR || result == TAJO_NO_MORE_DATA) {
      break;
    }
    num_rows++;

    for (int i = 0; i < column_count; i++) {
      resultset->GetText(i + 1, col_data, sizeof(col_data), &col_data_len, &is_null_value, err, sizeof(err));
      cout << string(col_data, 0, col_data_len) << ", ";
    }
    cout << endl;
  }
  cout << "num_rows:" << num_rows << endl;
  resultset->Close(err, sizeof(err));
  free(resultset);

  // Testing simple query
  TajoResultSet* resultset2;
  const char* sql2 = "select 1+1";
  cout << "\n===> ExecuteQuery: Eval query <===" << endl;
  call_status = ExecuteQuery(conn, sql2, &resultset2, err, sizeof(err));
  assert(call_status == TAJO_SUCCESS);

  num_rows = 0;
  while (1) {
    TajoReturn result = resultset2->Fetch(err, sizeof(err));
    if (result == TAJO_ERROR || result == TAJO_NO_MORE_DATA) {
      break;
    }
    num_rows++;

    for (int i = 0; i < 1; i++) {
      resultset2->GetText(i + 1, col_data, sizeof(col_data), &col_data_len, &is_null_value, err, sizeof(err));
      cout << string(col_data, 0, col_data_len) << ", ";
    }
    cout << endl;
  }
  cout << "num_rows:" << num_rows << endl;
  resultset2->Close(err, sizeof(err));
  free(resultset2);


  // Testing simple query
  TajoResultSet* resultset3;
  const char* sql3 = "select * from table2";
  cout << "\n===> ExecuteQuery: Non-forward query <===" << endl;
  call_status = ExecuteQuery(conn, sql3, &resultset3, err, sizeof(err));
  assert(call_status == TAJO_SUCCESS);

  num_rows = 0;
  while (1) {
    TajoReturn result = resultset3->Fetch(err, sizeof(err));
    if (result == TAJO_ERROR || result == TAJO_NO_MORE_DATA) {
      break;
    }
    num_rows++;

    for (int i = 0; i < 4; i++) {
      GetText(resultset3, i + 1, col_data, sizeof(col_data), &col_data_len, &is_null_value, err, sizeof(err));
      cout << string(col_data, 0, col_data_len) << ", ";
    }
    cout << endl;
  }
  cout << "num_rows:" << num_rows << endl;

  cout << "\n===> GetTajoColumn <===" << endl;
  for (int i = 0; i < 4; i++) {
    TajoColumn* tajo_column;
    GetTajoColumn(resultset3, i + 1, &tajo_column, err, sizeof(err));
    cout << tajo_column->column_name << "," << tajo_column->type_name << "," << tajo_column->type << endl;
    free(tajo_column);
  }


  resultset3->Close(err, sizeof(err));
  free(resultset3);

  //Close
  CloseConnection(conn, err, sizeof(err));

  return 0;
}

/*
boost
  ==> remove address-model=64
  sudo ./b2 threading=multi variant=release stage installsudo
thrift
  export CXX=clang++
  ./configure --prefix=/Users/babokim/work/program/thrift-0.9.2 --without-tests
  make
  (test 제거)

g++ -lthrift -Wall -L/Users/babokim/work/program/thrift-0.9.2/lib -I/Users/babokim/work/program/thrift-0.9.2/include -stdlib=libstdc++ \
./gen-cpp/tajo_constants.cpp ./gen-cpp/tajo_types.cpp ./gen-cpp/TajoThriftService.cpp \
main.cpp tajo_client.cc tajo_resultset.cc -o Client


g++ -Wall -stdlib=libstdc++ -I/Users/babokim/work/program/thrift-0.9.2/include \
-I/Users/babokim/ClionProjects/tajo-odbc/src/gen-cpp -I/Users/babokim/ClionProjects/tajo-odbc/src/cpp \
/Users/babokim/ClionProjects/tajo-odbc/src/test/test_tajo_client.cc \
-L/Users/babokim/ClionProjects/tajo-odbc/build/odbc/lib \
-L/Users/babokim/work/program/thrift-0.9.2/lib \
-ltajoclient -lthrift -o /Users/babokim/ClionProjects/tajo-odbc/build/odbc/test/test_tajo_client

 */