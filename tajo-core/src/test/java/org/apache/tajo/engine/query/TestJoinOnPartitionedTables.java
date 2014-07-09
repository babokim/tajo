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

import org.apache.tajo.QueryTestCaseBase;
import org.apache.tajo.TajoConstants;
import org.junit.Test;

import java.sql.ResultSet;

public class TestJoinOnPartitionedTables extends QueryTestCaseBase {

  public TestJoinOnPartitionedTables() {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
  }

  @Test
  public void testPartitionTableJoinSmallTable() throws Exception {
    executeDDL("customer_ddl.sql", null);
    ResultSet res = executeFile("insert_into_customer.sql");
    res.close();

    res = executeQuery();
    assertResultSet(res);
    res.close();

    res = executeFile("selfJoinOfPartitionedTable.sql");
    assertResultSet(res, "selfJoinOfPartitionedTable.result");
    res.close();

    res = executeFile("testNoProjectionJoinQual.sql");
    assertResultSet(res, "testNoProjectionJoinQual.result");
    res.close();

    res = executeFile("testPartialFilterPushDown.sql");
    assertResultSet(res, "testPartialFilterPushDown.result");
    res.close();

    res = executeFile("testPartialFilterPushDownOuterJoin.sql");
    assertResultSet(res, "testPartialFilterPushDownOuterJoin.result");
    res.close();

    res = executeFile("testPartialFilterPushDownOuterJoin2.sql");
    assertResultSet(res, "testPartialFilterPushDownOuterJoin2.result");
    res.close();

    executeString("DROP TABLE customer_parts PURGE").close();
  }

  @Test
  public void testFilterPushDownPartitionColumnCaseWhen() throws Exception {
    executeDDL("customer_ddl.sql", null);
    ResultSet res = executeFile("insert_into_customer.sql");
    res.close();

    res = executeQuery();
    assertResultSet(res);
    res.close();

    executeString("DROP TABLE customer_parts PURGE").close();
  }

  @Test
  public void testAAA() throws Exception {
    executeString(
        "create table td_sdp_pkt_data_f (svc_mgmt_num INT8, strd_dt TEXT, mth_acm_tot_length INT8, " +
            "tot_length INT8, mth_tot_rated_charg FLOAT8, tot_rated_charg FLOAT8, oper_dt_hms TEXT, data_exhst_cd TEXT) " +
            "PARTITION BY COLUMN(strd_ym TEXT)"
    ).close();

    executeString(
        "CREATE TABLE tm_f_sdp_app_use_line_anals " +
            "(svc_mgmt_num INT8, indv_corp_cl_cd TEXT, sex_cd TEXT, cust_age_cd TEXT, age_cl_cd TEXT, cust_typ_cd TEXT, " +
            "eqp_mdl_cd TEXT, eqp_ser_num TEXT, eqp_mfact_cd TEXT, eqp_acqr_ym TEXT, eqp_acqr_dt TEXT, eqp_grp_cd TEXT, " +
            "eqp_net_cl_cd TEXT, new_chg_cl_cd TEXT, eqp_use_ym_cnt INT4, eqp_use_ym_rng_cd TEXT, tmth_scrb_term_cl_cd TEXT, " +
            "tday_scrb_term_yn TEXT, svc_subsc_ym TEXT, svc_scrb_dt TEXT, scrb_req_rsn_cd TEXT, svc_scrb_term_yn TEXT, " +
            "svc_gr_cd TEXT, svc_term_ym TEXT, svc_term_dt TEXT, fee_prod_id TEXT, prod_grp_cd TEXT, " +
            "prod_grp_dtl_cd TEXT, prod_grp_knd_cd TEXT, prod_type_cd TEXT, prcpln_last_chg_mth TEXT, " +
            "prod_chg_cd TEXT, bas_ofr_data_qty INT8, pps_yn TEXT, equip_chg_day TEXT, equip_chg_yn TEXT, " +
            "mbr_3mth_use_yn TEXT, dom_cir_voice_call INT8, dom_cir_voice_use_tms INT8, " +
            "dom_cir_sms_call INT8, dom_cir_mms_call INT8, arpu INT8, arpu_amt_rng_cd TEXT, data_exhst_qty_cd TEXT, " +
            "data_exhst_qty INT8, data_exhst_yn TEXT, data_exhst_date TEXT, data_over_qty INT8, tb_prod_cd TEXT, " +
            "tb_prod_scrb_mth TEXT, tb_prod_term_mth TEXT, seoul_area_act_yn TEXT, oper_dt_hms TEXT) " +
            "PARTITION BY COLUMN(strd_ym TEXT)"
    ).close();

   executeString(
            "select count(bb.data_exhst_date)\n" +
                "from tm_f_sdp_app_use_line_anals a\n" +
                "left outer join\n" +
                "(select\n" +
                "    x.svc_mgmt_num as svc_mgmt_num\n" +
                "   ,sum(x.tot_length) as data_qty\n" +
                "  from td_sdp_pkt_data_f x\n" +
                "  where x.strd_ym='201405'\n" +
                "  group by x.svc_mgmt_num\n" +
                ") b\n" +
                "  on a.svc_mgmt_num = b.svc_mgmt_num\n" +
                "left outer join (\n" +
                "  select\n" +
                "    svc_mgmt_num\n" +
                "   ,min(y.strd_dt) as data_exhst_date\n" +
                "  from td_sdp_pkt_data_f y\n" +
                "  where  y.tot_rated_charg > 0\n" +
                "          and strd_ym = '201405'\n" +
                "  group by svc_mgmt_num\n" +
                ") bb\n" +
                "  on  a.svc_mgmt_num = bb.svc_mgmt_num\n" +
                "where a.strd_ym = '201405'"
   ).close();
  }
}
