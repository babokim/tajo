#
# Copyright 2005 The Apache Software Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Note: This makefile depends on 3 environment variables to function correctly:
# 1) WORD_SIZE
# 2) THRIFT_HOME
# 3) TAJO_CPP_ROOT
#

ifndef TAJO_CPP_ROOT
  BASE_DIR=.
else
  BASE_DIR=$(TAJO_CPP_ROOT)
endif

WORD_SIZE ?= 64
ifeq ($(WORD_SIZE),32)
  ARCH_FLAGS = -m32 -DARCH32
else
  ARCH_FLAGS = -m64 -DARCH64
endif

OS=$(shell uname -s)

ifeq ($(OS),Darwin)
  LDFLAGS = -dynamiclib -stdlib=libstdc++
  CXXFLAGS = -Wall -g -fPIC -stdlib=libstdc++
else
  LDFLAGS = -shared
  CXXFLAGS = -Wall -g -fPIC
endif


AR = ar
ARFLAGS = rcs
ARXFLAGS = -x
CC = gcc
CFLAGS = -Wall -g -fPIC
CXX = g++
LD = g++
INSTALL = /usr/bin/install -c
SHELL = /bin/sh
LIBTOOL = $(SHELL) /usr/bin/libtool
LINK = ln -sf

BUILD_DIR = $(BASE_DIR)/target
CLIENT_BUILD_DIR = $(BUILD_DIR)/tajoclient
OBJ_SERVICE_BUILD_DIR = $(BUILD_DIR)/thrift-service/objs
OBJ_CLIENT_BUILD_DIR = $(CLIENT_BUILD_DIR)/objs
LIB_CLIENT_BUILD_DIR = $(CLIENT_BUILD_DIR)/lib
TEST_CLIENT_BUILD_DIR = $(CLIENT_BUILD_DIR)/test
THRIFT_INCLUDE_PATH=$(THRIFT_HOME)/include
INSTALL_PATH = /usr/local
INSTALL_LIB_PATH = $(INSTALL_PATH)/lib
INSTALL_INCLUDE_PATH = $(INSTALL_PATH)/include

LIB_NAME = tajoclient
SHLIB_VERSION = 1.0.0
SO_LINK_NAME = lib$(LIB_NAME).so
SO_NAME = $(SO_LINK_NAME).$(SHLIB_VERSION)
SO_LINK_TARGET = $(LIB_CLIENT_BUILD_DIR)/$(SO_LINK_NAME)
SO_TARGET = $(LIB_CLIENT_BUILD_DIR)/$(SO_NAME) 
SO_INSTALL_LINK_TARGET = $(INSTALL_LIB_PATH)/$(SO_LINK_NAME)
SO_INSTALL_TARGET = $(INSTALL_LIB_PATH)/$(SO_NAME)
AR_NAME = lib$(LIB_NAME).a
AR_TARGET = $(LIB_CLIENT_BUILD_DIR)/$(AR_NAME)
AR_INSTALL_TARGET = $(INSTALL_LIB_PATH)/$(AR_NAME)

SERVICE_SRC_DIR = $(BASE_DIR)/src/main/gen-cpp
SERVICE_SOURCES = TajoThriftService.cpp \
                  tajo_constants.cpp \
                  tajo_types.cpp
SERVICE_OBJS = $(addprefix $(OBJ_SERVICE_BUILD_DIR)/,$(patsubst %,%.o,$(basename $(SERVICE_SOURCES))))

CLIENT_SRC_DIR = $(BASE_DIR)/src/main/cpp

CLIENT_SOURCES = tajo_client.cc \
               tajo_resultset.cc

CLIENT_OBJS = $(addprefix $(OBJ_CLIENT_BUILD_DIR)/,$(patsubst %,%.o,$(basename $(CLIENT_SOURCES))))

CLIENT_TEST_SRC_DIR = $(BASE_DIR)/src/test/cpp
#CLIENT_TEST_DATA_DIR = $(BASE_DIR)/testdata
#TEST_FLAGS = -DTEST_DATA_DIR=$(CLIENT_TEST_DATA_DIR)
TEST_FLAGS =
TAJO_CLIENT_TEST = $(TEST_CLIENT_BUILD_DIR)/test_tajo_client

INCLUDE_PATHS = -I$(THRIFT_INCLUDE_PATH) \
				-I$(SERVICE_SRC_DIR) \
                -I$(CLIENT_SRC_DIR)

INSTALL_HEADERS = tajo_client.h \
                  tajo_resultset.h \
                  tajo_thrift_connection.h

HEADER_SOURCES = $(addprefix $(CLIENT_SRC_DIR)/, $(INSTALL_HEADERS))
HEADER_TARGETS = $(INSTALL_INCLUDE_PATH)/tajo_client.h \
                  $(INSTALL_INCLUDE_PATH)/tajo_resultset.h \
                  $(INSTALL_INCLUDE_PATH)/tajo_thrift_connection.h

LIB_THRIFT_DIR = $(THRIFT_HOME)/lib
LIB_THRIFT_ADD = -L$(LIB_THRIFT_DIR) -lthrift
LIB_THRIFT_AR = $(LIB_THRIFT_DIR)/libthrift.a

#LIB_FB303_DIR = $(THRIFT_HOME)/lib
#LIB_FB303_ADD = -L$(LIB_FB303_DIR) -lfb303
#LIB_FB303_AR  = $(LIB_FB303_DIR)/libfb303.a

all:: $(AR_TARGET) $(SO_TARGET) $(TAJO_CLIENT_TEST)

$(AR_TARGET): $(SERVICE_OBJS) $(CLIENT_OBJS)
	if test -z '$(THRIFT_HOME)'; then echo 'THRIFT_HOME directory?'; exit 1; else exit 0; fi
	mkdir -p $(LIB_CLIENT_BUILD_DIR)
	$(AR) $(ARXFLAGS) $(LIB_THRIFT_AR) #Extract thrift archive
	$(AR) $(ARFLAGS) $@ $+ *.o #Include all object files into new archive
	rm *.o #Remove extracted archive object files

$(SO_TARGET): $(SERVICE_OBJS) $(CLIENT_OBJS)
	if test -z '$(THRIFT_HOME)'; then echo 'THRIFT_HOME directory?'; exit 1; else exit 0; fi
	mkdir -p $(LIB_CLIENT_BUILD_DIR)
	$(LD) $(ARCH_FLAGS) $(LDFLAGS) $+ $(LIB_THRIFT_ADD) -o $@ \
        && $(LINK) $(SO_NAME) $(SO_LINK_TARGET)


$(OBJ_SERVICE_BUILD_DIR)/%.o: $(SERVICE_SRC_DIR)/%.cpp
	mkdir -p $(OBJ_SERVICE_BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(ARCH_FLAGS) $(INCLUDE_PATHS) -c $< -o $@

$(OBJ_CLIENT_BUILD_DIR)/%.o: $(CLIENT_SRC_DIR)/%.cc
	mkdir -p $(OBJ_CLIENT_BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(ARCH_FLAGS) $(INCLUDE_PATHS) -c $< -o $@

$(TAJO_CLIENT_TEST): $(SO_TARGET) $(CLIENT_TEST_SRC_DIR)/test_tajo_client.cc
	mkdir -p $(TEST_CLIENT_BUILD_DIR)
	$(CXX) $(CXXFLAGS) $(ARCH_FLAGS) $(TEST_FLAGS) $(INCLUDE_PATHS) $(CLIENT_TEST_SRC_DIR)/test_tajo_client.cc -L$(LIB_CLIENT_BUILD_DIR) -L$(LIB_THRIFT_DIR) -l$(LIB_NAME) -lthrift -o $@


install: $(AR_TARGET) $(SO_TARGET)
	test -z "$(INSTALL_LIB_PATH)" || mkdir -p -- "$(INSTALL_LIB_PATH)"
	$(LIBTOOL) --mode=install $(INSTALL) '$(AR_TARGET)' '$(AR_INSTALL_TARGET)'
	$(LIBTOOL) --mode=install $(INSTALL) '$(SO_TARGET)' '$(SO_INSTALL_TARGET)' \
	&& $(LINK) $(SO_NAME) $(SO_INSTALL_LINK_TARGET)
	PATH="$PATH:/sbin" ldconfig -n $(INSTALL_LIB_PATH)
	test -z "$(INSTALL_INCLUDE_PATH)" || mkdir -p -- "$(INSTALL_INCLUDE_PATH)"
	$(INSTALL) -m 644 $(HEADER_SOURCES) $(INSTALL_INCLUDE_PATH)

uninstall:
	$(LIBTOOL) --mode=uninstall rm -f '$(AR_INSTALL_TARGET)' '$(SO_INSTALL_LINK_TARGET)' '$(SO_INSTALL_TARGET)'
	rm -f $(HEADER_TARGETS)

clean:
	rm -rf $(CLIENT_BUILD_DIR) $(OBJ_SERVICE_BUILD_DIR)

test: $(AR_TARGET) $(SO_TARGET) $(TAJO_CLIENT_TEST)
	LD_LIBRARY_PATH=$(LIB_CLIENT_BUILD_DIR):$(LIB_THRIFT_DIR):$(LD_LIBRARY_PATH) $(TAJO_CLIENT_TEST)
