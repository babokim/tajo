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

package org.apache.tajo.engine.query;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.apache.tajo.TajoTestingCluster;
import org.apache.tajo.catalog.Schema;
import org.apache.tajo.common.TajoDataTypes.Type;
import org.apache.tajo.conf.TajoConf.ConfVars;
import org.apache.tajo.storage.StorageConstants;
import org.apache.tajo.util.KeyValueSet;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
@RunWith(Parameterized.class)
public class TestJoinQuery extends QueryTestCaseBase {

  public TestJoinQuery(String joinOption) {
    super(TajoConstants.DEFAULT_DATABASE_NAME);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO.varname,
        ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname,
        ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(
        ConfVars.EXECUTOR_INNER_JOIN_INMEMORY_HASH_THRESHOLD.varname,
        ConfVars.EXECUTOR_INNER_JOIN_INMEMORY_HASH_THRESHOLD.defaultVal);

    testingCluster.setAllTajoDaemonConfValue(ConfVars.EXECUTOR_OUTER_JOIN_INMEMORY_HASH_THRESHOLD.varname,
        ConfVars.EXECUTOR_OUTER_JOIN_INMEMORY_HASH_THRESHOLD.defaultVal);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
        ConfVars.EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.defaultVal);

    if (joinOption.indexOf("NoBroadcast") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO.varname, "false");
      testingCluster.setAllTajoDaemonConfValue(ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname, "-1");
    }

    if (joinOption.indexOf("Hash") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(
          ConfVars.EXECUTOR_INNER_JOIN_INMEMORY_HASH_THRESHOLD.varname, String.valueOf(256 * 1048576));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.EXECUTOR_OUTER_JOIN_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(256 * 1048576));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(256 * 1048576));
    }
    if (joinOption.indexOf("Sort") >= 0) {
      testingCluster.setAllTajoDaemonConfValue(
          ConfVars.EXECUTOR_INNER_JOIN_INMEMORY_HASH_THRESHOLD.varname, String.valueOf(1));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.EXECUTOR_OUTER_JOIN_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(1));
      testingCluster.setAllTajoDaemonConfValue(ConfVars.EXECUTOR_GROUPBY_INMEMORY_HASH_THRESHOLD.varname,
          String.valueOf(1));
    }
  }

  @Parameters
  public static Collection<Object[]> generateParameters() {
    return Arrays.asList(new Object[][]{
        {"Hash_NoBroadcast"},
//        {"Sort_NoBroadcast"},
//        {"Hash"},
//        {"Sort"},
    });
  }

  @Test
  public final void testCrossJoin() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin5() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testWhereClauseJoin6() throws Exception {
    ResultSet res = executeQuery();
    System.out.println(resultSetToString(res));
    cleanupQuery(res);
  }

  @Test
  public final void testTPCHQ2Join() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinWithMultipleJoinQual1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinWithMultipleJoinQual2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinWithMultipleJoinQual3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinWithMultipleJoinQual4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoin1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithConstantExpr1() throws Exception {
    // outer join with constant projections
    //
    // select c_custkey, orders.o_orderkey, 'val' as val from customer
    // left outer join orders on c_custkey = o_orderkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithConstantExpr2() throws Exception {
    // outer join with constant projections
    //
    // select c_custkey, o.o_orderkey, 'val' as val from customer left outer join
    // (select * from orders) o on c_custkey = o.o_orderkey
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithConstantExpr3() throws Exception {
    // outer join with constant projections
    //
    // select a.c_custkey, 123::INT8 as const_val, b.min_name from customer a
    // left outer join ( select c_custkey, min(c_name) as min_name from customer group by c_custkey) b
    // on a.c_custkey = b.c_custkey;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testRightOuterJoin1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testFullOuterJoin1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testJoinCoReferredEvals1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testJoinCoReferredEvalsWithSameExprs1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testJoinCoReferredEvalsWithSameExprs2() throws Exception {
    // including grouping operator
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testCrossJoinAndCaseWhen() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testOuterJoinAndCaseWhen1() throws Exception {
    executeDDL("oj_table1_ddl.sql", "table1");
    executeDDL("oj_table2_ddl.sql", "table2");
    try {
      ResultSet res = executeQuery();
      assertResultSet(res);
      cleanupQuery(res);
    } finally {
      executeString("DROP TABLE table1");
      executeString("DROP TABLE table2");
    }
  }

  @Test
  public void testCrossJoinWithAsterisk1() throws Exception {
    // select region.*, customer.* from region, customer;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
   public void testCrossJoinWithAsterisk2() throws Exception {
    // select region.*, customer.* from customer, region;
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testCrossJoinWithAsterisk3() throws Exception {
    // select * from customer, region
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testCrossJoinWithAsterisk4() throws Exception {
    // select length(r_comment) as len, *, c_custkey*10 from customer, region order by len,r_regionkey,r_name
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testInnerJoinWithEmptyTable() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithEmptyTable1() throws Exception {
    /*
    select
      c_custkey,
      empty_orders.o_orderkey,
      empty_orders.o_orderstatus,
      empty_orders.o_orderdate
    from
      customer left outer join empty_orders on c_custkey = o_orderkey
    order by
      c_custkey, o_orderkey;
     */

    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithEmptyTable2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithEmptyTable3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithEmptyTable4() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithEmptyTable5() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testRightOuterJoinWithEmptyTable1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithEmptySubquery1() throws Exception {
    // Empty Null Supplying table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.put(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.put(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    String[] data = new String[]{ "1|table11-1", "2|table11-2", "3|table11-3", "4|table11-4", "5|table11-5" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 2);

    data = new String[]{ "1|table11-1", "2|table11-2" };
    TajoTestingCluster.createTable("table12", schema, tableOptions, data, 2);

    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.TESTCASE_MIN_TASK_NUM.varname, "2");

      ResultSet res = executeString("select a.id, b.id from table11 a " +
          "left outer join (" +
          "select table12.id from table12 inner join lineitem on table12.id = lineitem.l_orderkey and table12.id > 10) b " +
          "on a.id = b.id order by a.id");

      String expected = "id,id\n" +
          "-------------------------------\n" +
          "1,null\n" +
          "2,null\n" +
          "3,null\n" +
          "4,null\n" +
          "5,null\n";

      assertEquals(expected, resultSetToString(res));
      cleanupQuery(res);
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.TESTCASE_MIN_TASK_NUM.varname,
          ConfVars.TESTCASE_MIN_TASK_NUM.defaultVal);
      executeString("DROP TABLE table11 PURGE").close();
      executeString("DROP TABLE table12 PURGE").close();
    }
  }

  @Test
  public final void testLeftOuterJoinWithEmptySubquery2() throws Exception {
    //Empty Preserved Row table
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.put(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.put(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    String[] data = new String[]{ "1|table11-1", "2|table11-2", "3|table11-3", "4|table11-4", "5|table11-5" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data, 2);

    data = new String[]{ "1|table11-1", "2|table11-2" };
    TajoTestingCluster.createTable("table12", schema, tableOptions, data, 2);

    try {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.TESTCASE_MIN_TASK_NUM.varname, "2");

      ResultSet res = executeString("select a.id, b.id from " +
          "(select table12.id, table12.name, lineitem.l_shipdate " +
          "from table12 inner join lineitem on table12.id = lineitem.l_orderkey and table12.id > 10) a " +
          "left outer join table11 b " +
          "on a.id = b.id");

      String expected = "id,id\n" +
          "-------------------------------\n";

      assertEquals(expected, resultSetToString(res));
      cleanupQuery(res);
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.TESTCASE_MIN_TASK_NUM.varname,
          ConfVars.TESTCASE_MIN_TASK_NUM.defaultVal);
      executeString("DROP TABLE table11 PURGE");
      executeString("DROP TABLE table12 PURGE");
    }
  }

  @Test
  public final void testCrossJoinWithEmptyTable1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinOnMultipleDatabases() throws Exception {
    executeString("CREATE DATABASE JOINS");
    assertDatabaseExists("joins");
    executeString("CREATE TABLE JOINS.part_ as SELECT * FROM part");
    assertTableExists("joins.part_");
    executeString("CREATE TABLE JOINS.supplier_ as SELECT * FROM supplier");
    assertTableExists("joins.supplier_");
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE JOINS.part_ PURGE");
    executeString("DROP TABLE JOINS.supplier_ PURGE");
    executeString("DROP DATABASE JOINS");
  }

  @Test
  public final void testJoinWithJson() throws Exception {
    // select length(r_comment) as len, *, c_custkey*10 from customer, region order by len,r_regionkey,r_name
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinWithJson2() throws Exception {
    /*
    select t.n_nationkey, t.n_name, t.n_regionkey, t.n_comment, ps.ps_availqty, s.s_suppkey
    from (
      select n_nationkey, n_name, n_regionkey, n_comment
      from nation n
      join region r on (n.n_regionkey = r.r_regionkey)
    ) t
    join supplier s on (s.s_nationkey = t.n_nationkey)
    join partsupp ps on (s.s_suppkey = ps.ps_suppkey)
    where t.n_name in ('ARGENTINA','ETHIOPIA', 'MOROCCO');
     */
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testJoinOnMultipleDatabasesWithJson() throws Exception {
    executeString("CREATE DATABASE JOINS");
    assertDatabaseExists("joins");
    executeString("CREATE TABLE JOINS.part_ as SELECT * FROM part");
    assertTableExists("joins.part_");
    executeString("CREATE TABLE JOINS.supplier_ as SELECT * FROM supplier");
    assertTableExists("joins.supplier_");
    ResultSet res = executeJsonQuery();
    assertResultSet(res);
    cleanupQuery(res);

    executeString("DROP TABLE JOINS.part_ PURGE");
    executeString("DROP TABLE JOINS.supplier_ PURGE");
    executeString("DROP DATABASE JOINS");
  }

  @Test
  public final void testJoinAsterisk() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

//    executeString("DROP TABLE JOINS.part_ PURGE");
//    executeString("DROP TABLE JOINS.supplier_ PURGE");
//    executeString("DROP DATABASE JOINS");
  }

  @Test
  public final void testLeftOuterJoinWithNull1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithNull2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinWithNull3() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
          "from table11 t1\n" +
          "left outer join table12 t2\n" +
          "on t1.id = t2.id\n" +
          "left outer join table13 t3\n" +
          "on t1.id = t3.id and t2.id = t3.id");

      String expected =
          "id,name,id,id\n" +
          "-------------------------------\n" +
          "1,table11-1,1,null\n" +
          "2,table11-2,null,null\n" +
          "3,table11-3,null,null\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase2() throws Exception {
    // outer -> outer -> inner
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t2.id = t3.id\n" +
              "inner join table14 t4\n" +
              "on t2.id = t4.id"
      );

      String expected =
          "id,name,id,id,id\n" +
              "-------------------------------\n" +
              "1,table11-1,1,null,1\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase2_1() throws Exception {
    // inner(on predication) -> outer(on predication) -> outer -> where
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "inner join table14 t4\n" +
              "on t1.id = t4.id and t4.id > 1\n" +
              "left outer join table13 t3\n" +
              "on t4.id = t3.id and t3.id = 2\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id \n" +
              "where t1.id > 1"
      );

      String expected =
          "id,name,id,id,id\n" +
              "-------------------------------\n" +
              "2,table11-2,null,2,2\n" +
              "3,table11-3,null,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase3() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case J1: Join Predicate on Preserved Row Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2 \n" +
              "on t1.id = t2.id and (concat(t1.name, cast(t2.id as TEXT)) = 'table11-11' or concat(t1.name, cast(t2.id as TEXT)) = 'table11-33')\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id "
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "1,table11-1,1,null\n" +
              "2,table11-2,null,2\n" +
              "3,table11-3,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase4() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case J2: Join Predicate on Null Supplying Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id and t2.id > 1 \n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "1,table11-1,null,null\n" +
              "2,table11-2,null,2\n" +
              "3,table11-3,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase5() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case W1: Where Predicate on Preserved Row Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id\n" +
              "where t1.name > 'table11-1'"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "2,table11-2,null,2\n" +
              "3,table11-3,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testLeftOuterJoinPredicationCaseByCase6() throws Exception {
    // https://cwiki.apache.org/confluence/display/Hive/OuterJoinBehavior
    // Case W2: Where Predicate on Null Supplying Table
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "left outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "left outer join table13 t3\n" +
              "on t1.id = t3.id\n" +
              "where t3.id > 2"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "3,table11-3,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testRightOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "right outer join table12 t2\n" +
              "on t1.id = t2.id\n" +
              "right outer join table13 t3\n" +
              "on t1.id = t3.id and t2.id = t3.id"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "null,null,null,2\n" +
              "null,null,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testRightOuterJoinPredicationCaseByCase2() throws Exception {
    // inner -> right
    // Notice: Join order should be preserved with origin order.
    // JoinEdge: t1 -> t4, t3 -> t1,t4
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "inner join table14 t4\n" +
              "on t1.id = t4.id and t4.id > 1\n" +
              "right outer join table13 t3\n" +
              "on t4.id = t3.id and t3.id = 2\n" +
              "where t3.id > 1"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "2,table11-2,2,2\n" +
              "null,null,3,null\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testRightOuterJoinPredicationCaseByCase3() throws Exception {
    createOuterJoinTestTable();
    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t2.id, t3.id\n" +
              "from table11 t1\n" +
              "right outer join table12 t2 \n" +
              "on t1.id = t2.id and (concat(t1.name, cast(t2.id as TEXT)) = 'table11-11' or concat(t1.name, cast(t2.id as TEXT)) = 'table11-33')\n" +
              "right outer join table13 t3\n" +
              "on t1.id = t3.id "
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "null,null,null,2\n" +
              "null,null,null,3\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  @Test
  public final void testFullOuterJoinPredicationCaseByCase1() throws Exception {
    createOuterJoinTestTable();

    try {
      ResultSet res = executeString(
          "select t1.id, t1.name, t3.id, t4.id\n" +
              "from table11 t1\n" +
              "full outer join table13 t3\n" +
              "on t1.id = t3.id\n" +
              "full outer join table14 t4\n" +
              "on t3.id = t4.id \n" +
              "order by t4.id"
      );

      String expected =
          "id,name,id,id\n" +
              "-------------------------------\n" +
              "null,null,null,1\n" +
              "2,table11-2,2,2\n" +
              "3,table11-3,3,3\n" +
              "null,null,null,4\n" +
              "1,table11-1,null,null\n";

      String result = resultSetToString(res);

      assertEquals(expected, result);
    } finally {
      dropOuterJoinTestTable();
    }
  }

  private void createOuterJoinTestTable() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.put(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.put(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    String[] data = new String[]{ "1|table11-1", "2|table11-2", "3|table11-3" };
    TajoTestingCluster.createTable("table11", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    data = new String[]{ "1|table12-1" };
    TajoTestingCluster.createTable("table12", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    data = new String[]{"2|table13-2", "3|table13-3" };
    TajoTestingCluster.createTable("table13", schema, tableOptions, data);

    schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);
    data = new String[]{"1|table14-1", "2|table14-2", "3|table14-3", "4|table14-4" };
    TajoTestingCluster.createTable("table14", schema, tableOptions, data);
  }

  private void dropOuterJoinTestTable() throws Exception {
    executeString("DROP TABLE table11 PURGE;");
    executeString("DROP TABLE table12 PURGE;");
    executeString("DROP TABLE table13 PURGE;");
    executeString("DROP TABLE table14 PURGE;");
  }

  @Test
  public void testJoinWithEmptyData() throws Exception {
    ResultSet res = executeString(
        "select a.l_orderkey from (select * from lineitem where l_orderkey > 100) a \n" +
            "full outer join (select * from lineitem where l_orderkey > 100) b \n" +
            "on a.l_orderkey = b.l_orderkey"
    );

    String expected = "l_orderkey\n" +
        "-------------------------------\n";
    assertEquals(expected, resultSetToString(res));
  }

  @Test
  public void testAAA() throws Exception {
    executeString(
        "CREATE TABLE tm_s_sdp_skt_daily_tm_app_rpts " +
            "(app_grp_dtl_cd TEXT, seg_grp_dtl_cd TEXT, eqp_net_cl_cd TEXT, app_use_svc_cnt INT8, " +
            "app_use_svc_cnt_rnk INT4, app_use_svc_cnt_icdc_pct FLOAT4, app_use_svc_cnt_imprt_rt FLOAT4, " +
            "app_use_svc_cnt_tot_rnk INT4, avg_app_use_qty FLOAT8, avg_app_use_qty_rnk INT4, " +
            "avg_app_use_qty_icdc_pct FLOAT4, avg_app_use_qty_imprt_rt FLOAT4, " +
            "avg_app_use_qty_tot_rnk INT4, avg_app_use_tms FLOAT8, avg_app_use_tms_rnk INT4, " +
            "avg_app_use_tms_icdc_pct FLOAT4, avg_app_use_tms_imprt_rt FLOAT4, avg_app_use_tms_tot_rnk INT4, " +
            "avg_app_use_cnt FLOAT8, avg_app_use_cnt_rnk INT4, avg_app_use_cnt_icdc_pct FLOAT4, " +
            "avg_app_use_cnt_imprt_rt FLOAT4, avg_app_use_cnt_tot_rnk INT4, avg_app_use_dcnt FLOAT8, " +
            "avg_app_use_dcnt_rnk INT4, avg_app_use_dcnt_icdc_pct FLOAT4, avg_app_use_dcnt_imprt_rt FLOAT4, " +
            "avg_app_use_dcnt_tot_rnk INT4, app_grp_cnt INT4, oper_dt_hms TEXT) " +
            "PARTITION BY COLUMN(strd_ym TEXT, app_grp_cd TEXT, seg_grp_cd TEXT) "
    ).close();

    ResultSet res = executeString(
        "select a.app_grp_dtl_cd\n" +
            "     , a.seg_grp_dtl_cd\n" +
            "     , a.eqp_net_cl_cd\n" +
            "  from\n" +
            "       (select strd_ym\n" +
            "            , eqp_net_cl_cd\n" +
            "            , app_grp_cd\n" +
            "            , app_grp_dtl_cd\n" +
            "            , '10' seg_grp_cd\n" +
            "            , seg_grp_dtl_cd\n" +
            "         from tm_s_sdp_skt_daily_tm_app_rpts aa\n" +
            "        where aa.strd_ym = '201404'\n" +
            "              and aa.app_grp_cd = '33'\n" +
            "              and aa.seg_grp_cd = '01'\n" +
            "        and aa.seg_grp_cd = '01'\n" +
            "       ) a\n" +
            " left outer join tm_s_sdp_skt_daily_tm_app_rpts b\n" +
            "  on to_char(add_months(to_date(a.strd_ym, 'yyyymm'), -1), 'yyyymm') = b.strd_ym\n" +
            "       and a.eqp_net_cl_cd = b.eqp_net_cl_cd\n" +
            "       and a.app_grp_cd = b.app_grp_cd\n" +
            "       and a.app_grp_dtl_cd = b.app_grp_dtl_cd\n" +
            "       and a.seg_grp_cd = b.seg_grp_cd\n" +
            "       and a.seg_grp_dtl_cd = b.seg_grp_dtl_cd"
    );

    String expected = "app_grp_dtl_cd,seg_grp_dtl_cd,eqp_net_cl_cd\n" +
        "-------------------------------\n";
    assertEquals(expected, resultSetToString(res));

    res.close();
  }

  @Test
  public void testWindow() throws Exception {
    executeString(
        "CREATE TABLE tm_s_sdp_m_ctg_sum ( " +
            "svc_mgmt_num INT8, eqp_net_cl_cd TEXT, " +
            "app_ctg_lcl_cd TEXT, app_ctg_mcl_cd TEXT, age_cl_cd TEXT, sex_cd TEXT, prod_grp_cd TEXT, " +
            "prod_grp_dtl_cd TEXT, arpu_amt_rng_cd TEXT, data_exhst_qty_cd TEXT, user_data_use_cnt INT8, " +
            "user_data_upload_size INT8, user_data_download_size INT8, user_data_use_tms FLOAT8, app_use_dcnt INT8) " +
            "PARTITION BY COLUMN(strd_ym TEXT)"
    ).close();

    executeString(
        "CREATE TABLE tm_s_sdp_app_grp_cnt (ctg_cl_cd TEXT, app_ctg_mcl_cd TEXT, app_cnt INT4) " +
            "PARTITION BY COLUMN(strd_ym TEXT)"
    ).close();

    ResultSet res = executeString(
      "select x.app_ctg_mcl_cd as app_grp_dtl_cd\n" +
          "     , seg_grp_dtl_cd\n" +
          "     , eqp_net_cl_cd\n" +
          "     , app_use_svc_cnt\n" +
          "     , row_number() over(partition by strd_ym order by app_use_svc_cnt desc, app_ctg_mcl_cd asc) as app_use_svc_cnt_rnk\n" +
          "     , 0 as app_use_svc_cnt_icdc_pct\n" +
          "     , 100.00 as app_use_svc_cnt_imprt_rt\n" +
          "     , 0 as app_use_svc_cnt_tot_rnk\n" +
          "     , avg_app_use_qty\n" +
          "     , row_number() over(partition by strd_ym order by avg_app_use_qty desc, app_ctg_mcl_cd asc ) as avg_app_use_qty_rnk\n" +
          "     , 0 as avg_app_use_qty_icdc_pct\n" +
          "     , 100.00 as avg_app_use_qty_imprt_rt\n" +
          "     , 0 as avg_app_use_qty_tot_rnk\n" +
          "     , avg_app_use_tms\n" +
          "     , row_number() over(partition by strd_ym order by avg_app_use_tms desc, app_ctg_mcl_cd asc ) as avg_app_use_tms_rnk\n" +
          "     , 0 as avg_app_use_tms_icdc_pct\n" +
          "     , 100.00 as avg_app_use_tms_imprt_rt\n" +
          "     , 0 as avg_app_use_tms_tot_rnk\n" +
          "     , avg_app_use_cnt\n" +
          "     , row_number() over(partition by strd_ym order by avg_app_use_cnt desc, app_ctg_mcl_cd asc ) as avg_app_use_cnt_rnk\n" +
          "     , 0 as avg_app_use_cnt_icdc_pct\n" +
          "     , 100.00 as avg_app_use_cnt_imprt_rt\n" +
          "     , 0 as  avg_app_use_cnt_tot_rnk\n" +
          "     , avg_app_use_dcnt\n" +
          "     , row_number() over(partition by strd_ym order by avg_app_use_dcnt desc, app_ctg_mcl_cd asc ) as avg_app_use_dcnt_rnk\n" +
          "     , 0 as avg_app_use_dcnt_icdc_pct\n" +
          "     , 100.00 as avg_app_use_dcnt_imprt_rt\n" +
          "     , 0 as avg_app_use_dcnt_tot_rnk\n" +
          "     , app_grp_cnt\n" +
          "     , to_char(current_timestamp, 'yyyymmddhh24miss') as oper_dt_hms\n" +
          "     , strd_ym\n" +
          "     , app_grp_cd\n" +
          "     , seg_grp_cd\n" +
          "  from\n" +
          "       (select a.strd_ym\n" +
          "            , '**' as eqp_net_cl_cd\n" +
          "            , '11' as app_grp_cd\n" +
          "            , a.app_ctg_mcl_cd\n" +
          "            , '01' as seg_grp_cd\n" +
          "            , '**' as seg_grp_dtl_cd\n" +
          "            , count(distinct a.svc_mgmt_num) as app_use_svc_cnt\n" +
          "\t        , sum(user_data_upload_size + user_data_download_size)::float4 / count(distinct a.svc_mgmt_num)::float4 / 1024 / 1024 as avg_app_use_qty\n" +
          "\t        , sum(user_data_use_tms)::float4 / count(distinct a.svc_mgmt_num)::float4 / 60 as avg_app_use_tms\n" +
          "\t        , sum(user_data_use_cnt)::float4 / count(distinct a.svc_mgmt_num)::float4 as avg_app_use_cnt\n" +
          "\t        , sum(app_use_dcnt)::float4/count(distinct a.svc_mgmt_num)::float4 as avg_app_use_dcnt\n" +
          "\t        , b.app_grp_cnt\n" +
          "         from tm_s_sdp_m_ctg_sum a\n" +
          "            ,\n" +
          "              (select app_ctg_mcl_cd\n" +
          "                   , app_cnt app_grp_cnt\n" +
          "                from tm_s_sdp_app_grp_cnt\n" +
          "               where strd_ym = '201404'\n" +
          "                     and ctg_cl_cd = 'ALL'\n" +
          "              ) b\n" +
          "        where a.app_ctg_mcl_cd = b.app_ctg_mcl_cd\n" +
          "              and a.strd_ym = '201404'\n" +
          "        group by a.strd_ym,\n" +
          "                a.app_ctg_mcl_cd,              \n" +
          "                b.app_grp_cnt\n" +
          "       ) x"
    );

    assertEquals("app_grp_dtl_cd,seg_grp_dtl_cd,eqp_net_cl_cd,app_use_svc_cnt,app_use_svc_cnt_rnk,app_use_svc_cnt_icdc_pct,app_use_svc_cnt_imprt_rt,app_use_svc_cnt_tot_rnk,avg_app_use_qty,avg_app_use_qty_rnk,avg_app_use_qty_icdc_pct,avg_app_use_qty_imprt_rt,avg_app_use_qty_tot_rnk,avg_app_use_tms,avg_app_use_tms_rnk,avg_app_use_tms_icdc_pct,avg_app_use_tms_imprt_rt,avg_app_use_tms_tot_rnk,avg_app_use_cnt,avg_app_use_cnt_rnk,avg_app_use_cnt_icdc_pct,avg_app_use_cnt_imprt_rt,avg_app_use_cnt_tot_rnk,avg_app_use_dcnt,avg_app_use_dcnt_rnk,avg_app_use_dcnt_icdc_pct,avg_app_use_dcnt_imprt_rt,avg_app_use_dcnt_tot_rnk,app_grp_cnt,oper_dt_hms,strd_ym,app_grp_cd,seg_grp_cd\n" +
        "-------------------------------\n",
        resultSetToString(res));
    res.close();
  }

  @Test
  public void testJoinWithDifferentShuffleKey() throws Exception {
    KeyValueSet tableOptions = new KeyValueSet();
    tableOptions.put(StorageConstants.CSVFILE_DELIMITER, StorageConstants.DEFAULT_FIELD_DELIMITER);
    tableOptions.put(StorageConstants.CSVFILE_NULL, "\\\\N");

    Schema schema = new Schema();
    schema.addColumn("id", Type.INT4);
    schema.addColumn("name", Type.TEXT);

    List<String> data = new ArrayList<String>();

    int bytes = 0;
    for (int i = 0; i < 1000000; i++) {
      String row = i + "|" + i + "name012345678901234567890123456789012345678901234567890";
      bytes += row.getBytes().length;
      data.add(row);
      if (bytes > 2 * 1024 * 1024) {
        break;
      }
    }
    TajoTestingCluster.createTable("large_table", schema, tableOptions, data.toArray(new String[]{}));

    int originConfValue = conf.getIntVar(ConfVars.DIST_QUERY_JOIN_PARTITION_VOLUME);
    testingCluster.setAllTajoDaemonConfValue(ConfVars.DIST_QUERY_JOIN_PARTITION_VOLUME.varname, "1");
    ResultSet res = executeString(
        "select count(b.id) " +
            "from (select id, count(*) as cnt from large_table group by id) a " +
            "left outer join (select id, count(*) as cnt from large_table where id < 200 group by id) b " +
            "on a.id = b.id"
    );

    try {
      String expected =
          "?count\n" +
              "-------------------------------\n" +
              "200\n";

      assertEquals(expected, resultSetToString(res));
    } finally {
      testingCluster.setAllTajoDaemonConfValue(ConfVars.DIST_QUERY_JOIN_PARTITION_VOLUME.varname, "" + originConfValue);
      cleanupQuery(res);
      executeString("DROP TABLE large_table PURGE").close();
    }
  }

  @Test
  public final void testOnClauseJoin1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }
}
