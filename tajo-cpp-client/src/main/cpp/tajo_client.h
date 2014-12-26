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

/**
* @file tajo_client.h
* @brief Tajo client library interface.
*
* This header file defines the Tajo client library interface (C compatible).
*/

#ifndef TAJO_CLIENT_H_
#define TAJO_CLIENT_H_

#include "tajo_constants.h"

/**
* Enumeration of Tajo return values
*/
enum TajoReturn {
  TAJO_SUCCESS,
  TAJO_ERROR,
  TAJO_NO_MORE_DATA,
  TAJO_SUCCESS_WITH_MORE_DATA,
  TAJO_STILL_EXECUTING
};

/**
* Enumeration of known Tajo data types
*/
enum TajoDataType
{
  TAJO_BOOLEAN_TYPE,
  TAJO_INT1_TYPE,
  TAJO_INT2_TYPE,
  TAJO_INT4_TYPE,
  TAJO_INT8_TYPE,
  TAJO_FLOAT4_TYPE,
  TAJO_FLOAT8_TYPE,
  TAJO_TEXT_TYPE,
  TAJO_DATE_TYPE,
  TAJO_DATETIME_TYPE,
  TAJO_TIMESTAMP_TYPE,
  TAJO_UNKNOWN_TYPE
};

/**
* TColumn is cpp interface, so TajoColumn struct is defined for c client.
*/
struct TajoColumn {
  char* column_name;
  enum TajoDataType type;
  char* type_name;
};

/******************************************************************************
* Tajo Client C++ Class Placeholders
*****************************************************************************/
typedef enum TajoReturn TajoReturn;
typedef enum TajoDataType TajoDataType;
typedef struct TajoSessionVariable TajoSessionVariable;
typedef struct TajoColumn TajoColumn;

typedef struct TajoResultSet TajoResultSet;
typedef struct TajoThriftConnection TajoThriftConnection;

#ifdef __cplusplus
extern "C" {
#endif // __cplusplus

/**
 * @brief Connect to a Tajo database.
 *
 * Connects to a Tajo database on the specified Tajo thrift server. The caller takes
 * ownership of the returned TajoThriftConnection and is responsible for deallocating
 * the memory and connection by calling CloseConnection.
 *
 * @see CloseConnection()
 *
 * @param database    Name of the Tajo database on the Tajo thrift server to which to connect.
 * @param host        Host address of the Tajo thrift server.
 * @param port        Host port of the Tajo thrift server.
 * @param err         Buffer to receive an error message if TAJO_ERROR is returned.
 *                    NULL can be used if the caller does not care about the error message.
 * @param err_len     Size of the err buffer.
 *
 * @return A TajoThriftConnection object representing the established database connection,
 *         or NULL if an error occurred. Error messages will be stored in err.
 */
TajoThriftConnection* OpenConnection(const char* database, const char* host, int port, char* err, size_t err_len);

/**
 * @brief Disconnects from a Tajo thrift server.
 *
 * Disconnects from a Tajo thrift server and destroys the supplied TajoConnection object.
 * This function should eventually be called for every TajoConnection created by
 * DBOpenConnection.
 *
 * @see CloseConnection()
 *
 * @param conn      A TajoThriftConnection object associated a database connection.
 * @param err       Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len   Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 *         (error messages will be stored in err)
 */
TajoReturn CloseConnection(TajoThriftConnection* conn, char* err, size_t err_len);

/**
 * Create a database.
 * @param conn           A TajoThriftConnection object associated a database connection.
 * @param database_name  The database name to be created. This name is case sensitive.
 * @param err       Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len   Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn CreateDatabase(TajoThriftConnection* conn, const char* database_name, char* err, size_t err_len);

/**
 * Does the database exist?
 *
 * @param conn           A TajoThriftConnection object associated a database connection.
 * @param database_name  The database name to be checked. This name is case sensitive.
 * @param exists         The pointer to a int which will be set to 1 if exists, 0 if not exists.
 * @param err            Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len        Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn ExistDatabase(TajoThriftConnection* conn, const char* database_name, int* exists, char* err, size_t err_len);


/**
 * Drop the database
 *
 * @param conn           A TajoThriftConnection object associated a database connection.
 * @param database_name  The database name to be dropped. This name is case sensitive.
 * @param err            Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len        Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn DropDatabase(TajoThriftConnection* conn, const char* database_name, char* err, size_t err_len);

/**
 * Get a list of database names.
 *
 * @param conn           A TajoThriftConnection object associated a database connection.
 * @param databases      The pointer to a string list which will be set with database names.
 * @param num_databases  The pointer to a size_t which will be set with the number of database names.
 * @param err            Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len        Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn GetAllDatabaseNames(TajoThriftConnection* conn, char*** databases, size_t* num_databases,
  char* err, size_t err_len);

/**
 * Get a list of table names.
 * Caller takes ownership of returned TajoResultSet and is responsible for
 * deallocating the object by calling CloseResultSet.
 *
 * @param conn           A TajoThriftConnection object associated a database connection.
 * @param database_name  The database name to show all tables. This name is case sensitive.
 * @param table_name     The table name to filter. If null all tables will be return.
 * @param result_set     A pointer to a TajoResultSet pointer which will be associated with the result.
 * @param err            Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len        Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn GetTables(TajoThriftConnection* conn, const char* database_name, const char* table_name,
  TajoResultSet** result_set, char* err, size_t err_len);

/**
 * Drop a table.
 *
 * @param conn          A TajoThriftConnection object associated a database connection.
 * @param table_name   The table name to be dropped. This name is case sensitive.
 * @param purge        If purge is 1, this call will remove the entry in catalog as well as the table contents.
 * @param err          Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len      Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn DropTable(TajoThriftConnection* conn, const char* table_name, int purge, char* err, size_t err_len);

/**
 * Get columns in a table.
 * Caller takes ownership of returned TajoResultSet and is responsible for
 * deallocating the object by calling CloseResultSet.
 *
 * @param conn          A TajoThriftConnection object associated a database connection.
 * @param table_name    A target table name
 * @param result_set    A pointer to a TajoResultSet pointer which will be associated with the result.
 * @param err          Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len      Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn GetColumns(TajoThriftConnection* conn, const char* database_name, const char* table_name,
  TajoResultSet** result_set, char* err, size_t err_len);

/**
 * Get current database name.
 *
 * @param conn         A TajoThriftConnection object associated a database connection.
 * @param database     A pointer to a char array pointer which will be associated with current database.
 * @param err          Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len      Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn GetCurrentDatabase(TajoThriftConnection* conn, char* database, char* err, size_t err_len);

/**
 * Set current database.
 *
 * @param conn          A TajoThriftConnection object associated a database connection.
 * @param database      Database name will be selected.
 * @param err           Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len       Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn SelectDatabase(TajoThriftConnection* conn, const char* database, char* err, size_t err_len);

/**
 * Set the session variable.
 *
 * @param conn          A TajoThriftConnection object associated a database connection.
 * @param key           A key name which will be updated.
 * @param value         A value which will be updated.
 * @param err           Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len       Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn UpdateSessionVariable(TajoThriftConnection* conn, char *key, char *value, char* err, size_t err_len);

/**
 * Clear the session variable.
 *
 * @param conn          A TajoThriftConnection object associated a database connection.
 * @param key           A key name which will be clear.
 * @param err           Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len       Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn UnsetSessionVariable(TajoThriftConnection* conn, char *key, char* err, size_t err_len);

/**
 * Get all session variables.
 *
 * @param conn            A TajoThriftConnection object associated a database connection.
 * @param variables       A key name which will be clear.
 * @param num_variables   A pointer to a TajoSessionVariable array which will be contains session variable.
 *                        Caller is responsible for deallocating the object.
 * @param err             Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len         Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn GetAllSessionVariables(TajoThriftConnection* conn, TajoSessionVariable** variables, size_t* num_variables,
  char* err, size_t err_len);

/**
 * Check session status. If not created, call creating session.
 *
 * @param conn            A TajoThriftConnection object associated a database connection.
 * @param err             Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len         Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn CheckSessionAndGet(TajoThriftConnection* conn, char* err, size_t err_len);

/**
 * Execute a update query like create table or insert into.
 *
 * @param conn          A TajoThriftConnection object associated a database connection.
 * @param sql           The Tajo query string to be executed.
 * @param err           Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len       Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn UpdateQuery(TajoThriftConnection* conn, const char *sql, char* err, size_t err_len);

/**
 * @brief Execute a query.
 *
 * Executes a query on a Tajo connection and associates a TajoResultSet with the result.
 * Caller takes ownership of returned TajoResultSet and is responsible for deallocating
 * the object by calling CloseResultSet.
 *
 * @see CloseResultSet()
 *
 * @param conn          A TajoThriftConnection object associated a database connection.
 * @param query         The Tajo query string to be executed.
 * @param result_set    A pointer to a TajoResultSet pointer which will be associated with the
 *                      result, or NULL if the result is not needed.
 * @param err           Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len       Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn ExecuteQuery(TajoThriftConnection* conn, const char *sql,
 TajoResultSet** result_set, char* err, size_t err_len);

/**
 * @brief Fetches the next unfetched row in a TajoResultSet.
 *
 * Fetches the next unfetched row in a TajoResultSet. The fetched row will be stored
 * internally within the resultset and may be accessed through other DB functions.
 *
 * @param resultset   A TajoResultSet from which to fetch rows.
 * @param err_buf     Buffer to receive an error message if TAJO_ERROR is returned.
 *                    NULL can be used if the caller does not care about the error message.
 * @param err_buf_len Size of the err_buf buffer.
 *
 * @return TAJO_SUCCESS if successful,
 *         TAJO_ERROR if an error occurred,
 *         TJAO_NO_MORE_DATA if there are no more rows to fetch.
 *         (error messages will be stored in err_buf)
 */
TajoReturn Fetch(TajoResultSet* resultset, char* err, size_t err_len);

/**
 * @brief Destroys any specified TajoResultSet object.
 *
 * Destroys any specified TajoResultSet object. The TajoResultSet may have been
 * created by a number of other functions.
 *
 * @param resultset   A TajoResultSet object to be removed from memory.
 * @param err         Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len     Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred.
 */
TajoReturn CloseResultSet(TajoResultSet* resultset, char* err, size_t err_len);

/**
 * @brief Get a field as a C string.
 *
 * Reads out a field from the currently fetched rowset in a resultset as a C String.
 *
 * @param resultset       An initialized TajoResultSet.
 * @param column_index    Index of a column(starts with 1).
 * @param buffer          Pointer to a buffer that will receive the data.
 * @param buffer_len      Number of bytes allocated to buffer.
 * @param data_byte_size  Pointer to an size_t which will contain the length of the data available
 *                        to return before calling this function. Can be set to NULL if this
 *                        information is not needed.
 * @param is_null_value   Pointer to an int which will be set to 1 if the field contains a NULL value,
 *                        or 0 otherwise.
 * @param err_buf         Buffer to receive an error message if TAJO_ERROR is returned.
 *                        NULL can be used if the caller does not care about the error message.
 * @param err_buf_len     Size of the err_buf buffer.
 *
 * @return TAJO_SUCCESS if successful and there is no more data to fetch
 *         TAJO_ERROR if an error occurred (error messages will be stored in err_buf)
 *         TAJO_NO_MORE_DATA if this field has already been completely fetched
 */
TajoReturn GetText(TajoResultSet* resultset, size_t column_index, char *buffer, size_t buffer_len,
  size_t *data_byte_size, int *is_null_value, char *err, size_t err_len);

/**
 * @brief Get a field as an int.
 *
 * Reads out a field from the currently fetched rowset in a resultset as an int
 *       (platform specific).
 *
 * @param resultset     An initialized TajoResultSet.
 * @param column_index  Index of a column(starts with 1).
 * @param buffer        Pointer to an int that will receive the data.
 * @param is_null_value Pointer to an int which will be set to 1 if the field contains a NULL value,
 *                      or 0 otherwise
 * @param err           Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len       Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful and there is no more data to fetch
 *         TAJO_ERROR if an error occurred (error messages will be stored in err_buf)
 *         TAJO_NO_MORE_DATA if this field has already been completely fetched
 */
TajoReturn GetInt4(TajoResultSet* resultset, size_t column_index, int *buffer, int *is_null_value,
  char *err, size_t err_len);

/**
 * @brief Get a field as a long int.
 *
 * Reads out a field from the currently fetched rowset in a resultset as a long int
 *       (platform specific).
 *
 * @param resultset     An initialized TajoResultSet.
 * @param column_index  Index of a column(starts with 1).
 * @param buffer        Pointer to a long int that will receive the data.
 * @param is_null_value Pointer to an int which will be set to 1 if the field contains a NULL value,
 *                      or 0 otherwise
 * @param err           Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len       Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful and there is no more data to fetch
 *         TAJO_ERROR if an error occurred (error messages will be stored in err_buf)
 *         TAJO_NO_MORE_DATA if this field has already been completely fetched
 */
TajoReturn GetInt8(TajoResultSet* resultset, size_t column_index, long *buffer, int *is_null_value,
  char *err, size_t err_len);

/**
 * @brief Get a field as a double.
 *
 * Reads out a field from the currently fetched rowset in a resultset as a double
 *       (platform specific).
 *
 * @param resultset     An initialized TajoResultSet.
 * @param column_index  Index of a column(starts with 1).
 * @param buffer        Pointer to a double that will receive the data.
 * @param is_null_value Pointer to an int which will be set to 1 if the field contains a NULL value,
 *                      or 0 otherwise
 * @param err           Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len       Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful and there is no more data to fetch
 *         TAJO_ERROR if an error occurred (error messages will be stored in err_buf)
 *         TAJO_NO_MORE_DATA if this field has already been completely fetched
 */
TajoReturn GetFloat8(TajoResultSet* resultset, size_t column_index, double *buffer, int *is_null_value,
  char *err, size_t err_len);

/**
 * @brief Determines the number of columns in the TajoResultSet.
 *
 * @param resultset   A TajoResultSet from which to retrieve the column count.
 * @param col_count   Pointer to a size_t which will be set to the number of columns in the result.
 * @param err        Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len    Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred
 *         (error messages will be stored in err_buf)
 */
TajoReturn GetColumnCount(TajoResultSet* resultset, size_t* col_count, char* err, size_t err_len);

/**
 * @brief Get TajoColumn in a resultset.
 *
 * @param resultset     An initialized TajoResultSet.
 * @param column_index  Index of a column(starts with 1).
 * @param tajo_column   A pointer to a TajoColumn pointer which will receive the new TajoColumn.
 * @param err           Buffer to receive an error message if TAJO_ERROR is returned.
 * @param err_len       Size of the err buffer.
 *
 * @return TAJO_SUCCESS if successful, or TAJO_ERROR if an error occurred
 *         (error messages will be stored in err_buf)
 */
TajoReturn GetTajoColumn(TajoResultSet* resultset, size_t column_index, TajoColumn** tajo_column,
  char *err, size_t err_len);

/**
 * @brief Finds the max display size of a column's fields.
 *
 * @param type A type associated with the column in question.
 *
 * @return The maximum number of characters needed to represent a field within this column.
 */
size_t GetMaxDisplaySize(TajoDataType type);

/**
 * @brief Finds the max (native) byte size of a column's fields.
 *
 * @param type A type associated with the column in question.
 *
 * @return The number of bytes needed to store a field within this column in its native type.
 */
size_t GetFieldByteSize(TajoDataType type);

#ifdef __cplusplus
} // extern "C"
#endif // __cpluscplus

#endif  // TAJO_CLIENT_H_