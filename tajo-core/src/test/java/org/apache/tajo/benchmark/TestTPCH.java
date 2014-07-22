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

package org.apache.tajo.benchmark;

import org.apache.tajo.IntegrationTest;
import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;

@Category(IntegrationTest.class)
public class TestTPCH extends QueryTestCaseBase {

  public TestTPCH() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public void testQ1OrderBy() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testQ2FourJoins() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void testTPCH14Expr() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public void textTPCH2() throws Exception {
    String q1 = "create table q2_minimum_cost_supplier_tmp1 (s_acctbal double, s_name text, n_name text, p_partkey int8, ps_supplycost double, p_mfgr text, s_address text, s_phone text, s_comment text)";
    String q2 = "create table q2_minimum_cost_supplier_tmp2 (p_partkey int8, ps_min_supplycost double)";
    String q3 = "create table q2_minimum_cost_supplier (s_acctbal double, s_name text, n_name text, p_partkey int8, p_mfgr text, s_address text, s_phone text, s_comment text)";

    executeString(q1).close();
    executeString(q2).close();
    executeString(q3).close();

    ResultSet res = executeString(
        "insert overwrite into q2_minimum_cost_supplier \n" +
            "select \n" +
            "  t1.s_acctbal, t1.s_name, t1.n_name, t1.p_partkey, t1.p_mfgr, t1.s_address, t1.s_phone, t1.s_comment \n" +
            "from \n" +
            "  q2_minimum_cost_supplier_tmp1 t1 join q2_minimum_cost_supplier_tmp2 t2 \n" +
            "on \n" +
            "  t1.p_partkey = t2.p_partkey and t1.ps_supplycost=t2.ps_min_supplycost \n" +
            "order by s_acctbal desc, n_name, s_name, p_partkey \n" +
            "limit 100"
    );

    assertEquals("", resultSetToString(res));

    res.close();
  }
}