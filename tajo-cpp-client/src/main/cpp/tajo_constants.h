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

#ifndef TAJO_CONSTANTS_H_
#define TAJO_CONSTANTS_H_

#include "tajo_client.h"

/// Maximum length of a column name
static const int MAX_COLUMN_NAME_LEN  = 64;
/// Maximum length of a column type name
static const int MAX_COLUMN_TYPE_LEN  = 64;

/// Maximum number of characters needed to display any field
static const int MAX_DISPLAY_SIZE = 334;
/// Maximum number of bytes needed to store any field
static const int MAX_BYTE_LENGTH  = 334;

/* Default connection parameters */
static const char* DEFAULT_DATABASE = "default";
static const char* DEFAULT_HOST     = "localhost";
static const char* DEFAULT_PORT     = "26700";

/* Default variable */
static const int DEFAULT_FETCH_SIZE = 100;
static const int MAX_TAJO_ERR_MSG_LEN = 128;

/**
* Structure of Tajo session variable
*/
struct TajoSessionVariable {
  char* key;
  char* value;
};

#define LENGTH(arr) (sizeof(arr)/sizeof(arr[0]))

/**
* Checks an error condition, and if true:
* 1. prints the error
* 2. saves the message to err_buf
* 3. returns the specified ret_val
*/
#define RETURN_ON_ASSERT(condition, funct_name, error_msg, err, err_en, ret_val) {     \
  if (condition) {                                                                     \
      cerr << funct_name << ": " << error_msg << endl << flush;                        \
      strncpy(err, error_msg, err_len);                                           \
      return ret_val;                                                                  \
  }                                                                                    \
}

/**
* Always performs the following:
* 1. prints the error
* 2. saves the message to err_buf
* 3. returns the specified ret_val
*/
#define RETURN_FAILURE(funct_name, error_msg, err, err_len, ret_val) {  \
  RETURN_ON_ASSERT(true, funct_name, error_msg, err, err_len, ret_val); \
}

#endif // TAJO_CONSTANTS_H_