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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.tajo.*;
import org.apache.tajo.catalog.*;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.datum.Datum;
import org.apache.tajo.datum.Int4Datum;
import org.apache.tajo.datum.TextDatum;
import org.apache.tajo.engine.planner.global.ExecutionBlock;
import org.apache.tajo.engine.planner.global.MasterPlan;
import org.apache.tajo.engine.planner.logical.NodeType;
import org.apache.tajo.jdbc.TajoResultSet;
import org.apache.tajo.master.querymaster.QueryMasterTask;
import org.apache.tajo.master.querymaster.SubQuery;
import org.apache.tajo.storage.Appender;
import org.apache.tajo.storage.StorageManagerFactory;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.storage.VTuple;
import org.apache.tajo.util.FileUtil;
import org.apache.tajo.worker.TajoWorker;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.Iterator;

import static junit.framework.TestCase.*;
import static junit.framework.TestCase.assertEquals;
import static org.apache.tajo.ipc.TajoWorkerProtocol.ShuffleType.*;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

@Category(IntegrationTest.class)
public class TestJoinBroadcast extends QueryTestCaseBase {
  private static final Log LOG = LogFactory.getLog(TestJoinBroadcast.class);
  public TestJoinBroadcast() throws Exception {
    super(TajoConstants.DEFAULT_DATABASE_NAME);
    testingCluster.setAllTajoDaemonConfValue(TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_AUTO.varname, "true");
    testingCluster.setAllTajoDaemonConfValue(
        TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD.varname,
        TajoConf.ConfVars.DIST_QUERY_BROADCAST_JOIN_THRESHOLD.defaultVal);

    executeDDL("create_lineitem_large_ddl.sql", "lineitem_large");
    executeDDL("create_customer_large_ddl.sql", "customer_large");
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
  public final void testLeftOuterJoin1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testLeftOuterJoin2() throws Exception {
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
    // select length(r_regionkey), *, c_custkey*10 from customer, region
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
  public final void testRightOuterJoinWithEmptyTable1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testFullOuterJoinWithEmptyTable1() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
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

  private MasterPlan getQueryPlan(QueryId queryId) {
    for (TajoWorker eachWorker: testingCluster.getTajoWorkers()) {
      QueryMasterTask queryMasterTask = eachWorker.getWorkerContext().getQueryMaster().getQueryMasterTask(queryId, true);
      if (queryMasterTask != null) {
        return queryMasterTask.getQuery().getPlan();
      }
    }

    fail("Can't find query from workers" + queryId);
    return null;
  }

  @Test
  public final void testBroadcastBasicJoin() throws Exception {
    ResultSet res = executeQuery();
    TajoResultSet ts = (TajoResultSet)res;
    assertResultSet(res);
    cleanupQuery(res);

    MasterPlan plan = getQueryPlan(ts.getQueryId());
    ExecutionBlock rootEB = plan.getRoot();

    /*
    |-eb_1395998037360_0001_000006
       |-eb_1395998037360_0001_000005
     */
    assertEquals(1, plan.getChildCount(rootEB.getId()));

    ExecutionBlock firstEB = plan.getChild(rootEB.getId(), 0);

    assertNotNull(firstEB);
    assertEquals(2, firstEB.getBroadcastTables().size());
    assertTrue(firstEB.getBroadcastTables().contains("default.supplier"));
    assertTrue(firstEB.getBroadcastTables().contains("default.part"));
  }

  @Test
  public final void testBroadcastTwoPartJoin() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);

    MasterPlan plan = getQueryPlan(((TajoResultSet)res).getQueryId());
    ExecutionBlock rootEB = plan.getRoot();

    /*
    |-eb_1395996354406_0001_000010
       |-eb_1395996354406_0001_000009
          |-eb_1395996354406_0001_000008
          |-eb_1395996354406_0001_000005
     */
    assertEquals(1, plan.getChildCount(rootEB.getId()));

    ExecutionBlock firstJoinEB = plan.getChild(rootEB.getId(), 0);
    assertNotNull(firstJoinEB);
    assertEquals(NodeType.JOIN, firstJoinEB.getPlan().getType());
    assertEquals(0, firstJoinEB.getBroadcastTables().size());

    ExecutionBlock leafEB1 = plan.getChild(firstJoinEB.getId(), 0);
    assertTrue(leafEB1.getBroadcastTables().contains("default.orders"));
    assertTrue(leafEB1.getBroadcastTables().contains("default.part"));

    ExecutionBlock leafEB2 = plan.getChild(firstJoinEB.getId(), 1);
    assertTrue(leafEB2.getBroadcastTables().contains("default.nation"));
  }

  @Test
  public final void testBroadcastSubquery() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  @Test
  public final void testBroadcastSubquery2() throws Exception {
    ResultSet res = executeQuery();
    assertResultSet(res);
    cleanupQuery(res);
  }

  private Collection<SubQuery> getSubQueries(QueryId queryId) {
    for (TajoWorker eachWorker: testingCluster.getTajoWorkers()) {
      QueryMasterTask queryMasterTask = eachWorker.getWorkerContext().getQueryMaster().getQueryMasterTask(queryId, true);
      if (queryMasterTask != null) {
        return queryMasterTask.getQuery().getSubQueries();
      }
    }

    fail("Can't find query from workers" + queryId);
    return null;
  }
  @Test
  public final void testBroadcastPartitionTable() throws Exception {
    // https://issues.apache.org/jira/browse/TAJO-839
    // If all tables participate in the BROADCAST JOIN, there is some missing data.
    executeDDL("customer_partition_ddl.sql", null);
    ResultSet res = executeFile("insert_into_customer_partition.sql");
    res.close();
    createMultiFile("nation", 2, new TupleCreator() {
      public Tuple createTuple(String[] columnDatas) {
        return new VTuple(new Datum[]{
            new Int4Datum(Integer.parseInt(columnDatas[0])),
            new TextDatum(columnDatas[1]),
            new Int4Datum(Integer.parseInt(columnDatas[2])),
            new TextDatum(columnDatas[3])
        });
      }
    });

    createMultiFile("orders", 1, new TupleCreator() {
      public Tuple createTuple(String[] columnDatas) {
        return new VTuple(new Datum[]{
            new Int4Datum(Integer.parseInt(columnDatas[0])),
            new Int4Datum(Integer.parseInt(columnDatas[1])),
            new TextDatum(columnDatas[2])
        });
      }
    });

    res = executeQuery();
    assertResultSet(res);
    res.close();

    executeString("DROP TABLE customer_broad_parts PURGE");
    executeString("DROP TABLE nation_multifile PURGE");
    executeString("DROP TABLE orders_multifile PURGE");
  }

//  @Test
//  public final void testBroadcastPartitionTable2() throws Exception {
//    // small(partition), large(partition), small
//    executeDDL("lineitem_large_partition_ddl.sql", null);
//    executeDDL("customer_partition_ddl.sql", null);
//
//    executeFile("insert_into_customer_partition.sql").close();
//    executeFile("insert_into_lineitem_large_partition.sql").close();
//
//    ResultSet res = executeString(
//        "select * from customer_broad_parts a " +
//            "left outer join lineitem_large_parts b on cast(a.c_name as INT4) = b.l_partkey and b.l_orderkey = 1 " +
//            "left outer join orders c on b.l_suppkey = c.o_orderkey "  +
//            "where a.c_custkey > 2"
//    );
//    assertResultSet(res);
//    res.close();
//
//    executeString("DROP TABLE customer_broad_parts PURGE");
//    executeString("DROP TABLE lineitem_large_parts PURGE");
//  }

  @Test
  public final void testInnerAndOuterWithEmpty() throws Exception {
    executeDDL("customer_partition_ddl.sql", null);
    executeFile("insert_into_customer_partition.sql").close();

    // outer join table is empty
    ResultSet res = executeString(
        "select a.l_orderkey, b.o_orderkey, c.c_custkey from lineitem a " +
            "inner join orders b on a.l_orderkey = b.o_orderkey " +
            "left outer join customer_broad_parts c on a.l_orderkey = c.c_custkey and c.c_custkey < 0"
    );

    String expected = "l_orderkey,o_orderkey,c_custkey\n" +
        "-------------------------------\n" +
        "1,1,null\n" +
        "1,1,null\n" +
        "2,2,null\n" +
        "3,3,null\n" +
        "3,3,null\n";

    assertEquals(expected, resultSetToString(res));
    res.close();

    executeString("DROP TABLE customer_broad_parts PURGE").close();
  }
  @Test
  public final void testCasebyCase1() throws Exception {
    // Left outer join with a small table and a large partition table which not matched any partition path.
    String tableName = CatalogUtil.normalizeIdentifier("largePartitionedTable");
    testBase.execute(
        "create table " + tableName + " (l_partkey int4, l_suppkey int4, l_linenumber int4, \n" +
            "l_quantity float8, l_extendedprice float8, l_discount float8, l_tax float8, \n" +
            "l_returnflag text, l_linestatus text, l_shipdate text, l_commitdate text, \n" +
            "l_receiptdate text, l_shipinstruct text, l_shipmode text, l_comment text) \n" +
            "partition by column(l_orderkey int4) ").close();
    TajoTestingCluster cluster = testBase.getTestingCluster();
    CatalogService catalog = cluster.getMaster().getCatalog();
    assertTrue(catalog.existsTable(TajoConstants.DEFAULT_DATABASE_NAME, tableName));

    executeString("insert overwrite into " + tableName +
        " select l_partkey, l_suppkey, l_linenumber, \n" +
        " l_quantity, l_extendedprice, l_discount, l_tax, \n" +
        " l_returnflag, l_linestatus, l_shipdate, l_commitdate, \n" +
        " l_receiptdate, l_shipinstruct, l_shipmode, l_comment, l_orderkey from lineitem_large");

    ResultSet res = executeString(
        "select a.l_orderkey as key1, b.l_orderkey as key2 from lineitem as a " +
            "left outer join " + tableName + " b " +
            "on a.l_partkey = b.l_partkey and b.l_orderkey = 1000"
    );

    String expected = "key1,key2\n" +
        "-------------------------------\n" +
        "1,null\n" +
        "1,null\n" +
        "2,null\n" +
        "3,null\n" +
        "3,null\n";

    try {
      assertEquals(expected, resultSetToString(res));
    } finally {
      cleanupQuery(res);
    }
  }

  static interface TupleCreator {
    public Tuple createTuple(String[] columnDatas);
  }

  private void createMultiFile(String tableName, int numRowsEachFile, TupleCreator tupleCreator) throws Exception {
    // make multiple small file
    String multiTableName = tableName + "_multifile";
    executeDDL(multiTableName + "_ddl.sql", null);

    TableDesc table = client.getTableDesc(multiTableName);
    assertNotNull(table);

    TableMeta tableMeta = table.getMeta();
    Schema schema = table.getLogicalSchema();

    File file = new File("src/test/tpch/" + tableName + ".tbl");

    if (!file.exists()) {
      file = new File(System.getProperty("user.dir") + "/tajo-core/src/test/tpch/" + tableName + ".tbl");
    }
    String[] rows = FileUtil.readTextFile(file).split("\n");

    assertTrue(rows.length > 0);

    int fileIndex = 0;

    Appender appender = null;
    for (int i = 0; i < rows.length; i++) {
      if (i % numRowsEachFile == 0) {
        if (appender != null) {
          appender.flush();
          appender.close();
        }
        Path dataPath = new Path(table.getPath(), fileIndex + ".csv");
        fileIndex++;
        appender = StorageManagerFactory.getStorageManager(conf).getAppender(tableMeta, schema,
            dataPath);
        appender.init();
      }
      String[] columnDatas = rows[i].split("\\|");
      Tuple tuple = tupleCreator.createTuple(columnDatas);
      appender.addTuple(tuple);
    }
    appender.flush();
    appender.close();
  }

//  @Test
//  public void testAAA() throws Exception {
//    String table1 =
//        "CREATE TABLE td_sdp_ps_daily_sum (svc_mgmt_num INT8, net_cl_cd TEXT, package_id TEXT, " +
//            "law_area_cd TEXT, lct_cl_cd TEXT, download_cnt INT8, data_upload_size INT8, " +
//            "data_download_size INT8, data_use_tms FLOAT8, header_use_size INT8, oper_dt_hms TEXT) " +
//            "PARTITION BY COLUMN(strd_dt TEXT, tm_rng_cd TEXT) ";
//
//    String table2 =
//        "CREATE TABLE tm_d_sdp_law_area_cd (law_area_cd TEXT, law_pvc_cd TEXT, law_pvc_nm TEXT, " +
//            "law_gun_gu_cd TEXT, law_gun_gu_nm TEXT, law_dong_cd TEXT, law_dong_nm TEXT, " +
//            "law_ri_cd TEXT, law_ri_nm TEXT, oper_dt_hms TEXT)" ;
//
//    String table3 =
//        "CREATE TABLE tm_f_sdp_app_use_line_anals (svc_mgmt_num INT8, indv_corp_cl_cd TEXT, " +
//            "sex_cd TEXT, cust_age_cd TEXT, age_cl_cd TEXT, cust_typ_cd TEXT, eqp_mdl_cd TEXT," +
//            "eqp_ser_num TEXT, eqp_mfact_cd TEXT, eqp_acqr_ym TEXT, eqp_acqr_dt TEXT, eqp_grp_cd TEXT, " +
//            "eqp_net_cl_cd TEXT, new_chg_cl_cd TEXT, eqp_use_ym_cnt INT4, eqp_use_ym_rng_cd TEXT, " +
//            "tmth_scrb_term_cl_cd TEXT, tday_scrb_term_yn TEXT, svc_subsc_ym TEXT, " +
//            "svc_scrb_dt TEXT, scrb_req_rsn_cd TEXT, svc_scrb_term_yn TEXT, svc_gr_cd TEXT, " +
//            "svc_term_ym TEXT, svc_term_dt TEXT, fee_prod_id TEXT, prod_grp_cd TEXT, " +
//            "prod_grp_dtl_cd TEXT, prod_grp_knd_cd TEXT, prod_type_cd TEXT, prcpln_last_chg_mth TEXT, " +
//            "prod_chg_cd TEXT, bas_ofr_data_qty INT8, pps_yn TEXT, equip_chg_day TEXT, equip_chg_yn TEXT, " +
//            "mbr_3mth_use_yn TEXT, dom_cir_voice_call INT8, dom_cir_voice_use_tms INT8, " +
//            "dom_cir_sms_call INT8, dom_cir_mms_call INT8, arpu INT8, arpu_amt_rng_cd TEXT, " +
//            "data_exhst_qty_cd TEXT, data_exhst_qty INT8, data_exhst_yn TEXT, data_exhst_date TEXT, " +
//            "data_over_qty INT8, tb_prod_cd TEXT, tb_prod_scrb_mth TEXT, tb_prod_term_mth TEXT, " +
//            "seoul_area_act_yn TEXT, oper_dt_hms TEXT) " +
//            "PARTITION BY COLUMN(strd_ym TEXT) ";
//
//    //small
//    String table4 =
//        "CREATE TABLE tm_d_sdp_ps_app_category (ps_app_group TEXT, package_id TEXT, app_nm TEXT, price FLOAT4, price_cd TEXT, oper_dt_hms TEXT) ";
//
//    //small(1.1 MB)
//    String table5 =
//        "CREATE TABLE tc_sdp_cldr_cd (cldr_cd TEXT, cldr_dt TEXT, lnr_dt TEXT, hday_yn TEXT, leapm_yn TEXT, optms_cd TEXT, dow_cd TEXT, oper_dt_hms TEXT)  ";
//
//    executeString(table1).close();
//    executeString(table2).close();
//    executeString(table3).close();
//    executeString(table4).close();
//    executeString(table5).close();
//
//    CatalogService catalog = testingCluster.getMaster().getCatalog();
//    String[] tableNames = new String[]{"td_sdp_ps_daily_sum", "tm_d_sdp_law_area_cd", "tm_f_sdp_app_use_line_anals",
//        "tm_d_sdp_ps_app_category", "tc_sdp_cldr_cd"};
//    Path aaa = null;
//    for (String eachTable: tableNames) {
//      TableDesc tableDesc = catalog.getTableDesc(TajoConstants.DEFAULT_DATABASE_NAME, eachTable);
//
//      Path tablePath = tableDesc.getPath();
//      FileSystem fs = tablePath.getFileSystem(conf);
//      //fs.delete(tablePath, true);
//      Path localPath = new Path("file:///Users/seungunchoe/Downloads/Tajo_Patch/babokim/data/" + eachTable);
//      FileSystem localFs = FileSystem.getLocal(conf);
//      for (FileStatus eachFile: localFs.listStatus(localPath)) {
//        fs.copyFromLocalFile(eachFile.getPath(), tablePath);
//      }
//    }
//
//    ResultSet res = executeString(
//        "select a.svc_mgmt_num                             AS svc_mgmt_num\n" +
//            "       ,a.net_cl_cd                                AS net_cl_cd\n" +
//            "       ,Coalesce(c.age_cl_cd, '##')                AS age_cl_cd\n" +
//            "       ,Coalesce(Substr(c.equip_chg_day, 6), '##') AS eqp_chg_ym\n" +
//            "       ,Coalesce(c.eqp_use_ym_cnt, 0)              AS eqp_use_ym\n" +
//            "       ,Coalesce(c.eqp_use_ym_rng_cd, '##')        AS eqp_use_ym_rng_cd\n" +
//            "       \n" +
//            "       ,Substr(a.strd_dt, 1, 6)                    AS strd_ym\n" +
//            "       ,a.strd_dt                                  AS strd_dt\n" +
//            "       ,a.tm_rng_cd                                AS tm_rng_cd\n" +
//            "FROM   td_sdp_ps_daily_sum a\n" +
//            "      LEFT OUTER JOIN tm_d_sdp_law_area_cd b\n" +
//            "                    ON a.law_area_cd = b.law_area_cd\n" +
//            "       LEFT OUTER JOIN tm_f_sdp_app_use_line_anals c\n" +
//            "                    ON c.strd_ym = '201404'\n" +
//            "                       AND a.svc_mgmt_num = c.svc_mgmt_num\n" +
//            "       LEFT OUTER JOIN tm_d_sdp_ps_app_category d\n" +
//            "                    ON a.package_id = d.package_id\n" +
//            "       LEFT OUTER JOIN tc_sdp_cldr_cd e\n" +
//            "                    ON a.strd_dt = e.cldr_dt\n" +
//            "                       AND e.cldr_cd = 'WKNO'"
//    );
//
//    String expected =
//        "svc_mgmt_num,net_cl_cd,age_cl_cd,eqp_chg_ym,eqp_use_ym,eqp_use_ym_rng_cd,strd_ym,strd_dt,tm_rng_cd\n" +
//            "-------------------------------\n" +
//            "7217690798,3G,07,231,22,10,201404,20140414,10\n" +
//            "7217689641,3G,06,231,22,10,201404,20140414,10\n" +
//            "7217690734,3G,05,231,22,10,201404,20140414,10\n" +
//            "7217689641,3G,06,231,22,10,201404,20140414,10\n" +
//            "7217691231,3G,07,231,22,10,201404,20140414,10\n" +
//            "7217690465,3G,01,231,22,10,201404,20140414,10\n" +
//            "7217689027,3G,07,231,22,10,201404,20140414,10\n" +
//            "7217689366,3G,02,231,22,10,201404,20140414,10\n" +
//            "7217689637,4G,05,107,15,07,201404,20140414,10\n" +
//            "7217690798,3G,07,231,22,10,201404,20140414,10\n";
//    assertEquals(expected, resultSetToString(res));
//
//    res.close();
//  }
//
//  @Test
//  public void testSort() throws Exception {
//    executeString("create table vctaoz (dis_app_grp_nm	TEXT, appindex_grp_cd	TEXT, t50b30503744372	TEXT, seg_grp_cd	TEXT, " +
//        "data_exhst_qty_cd1	TEXT, t50b307b3744373	INT8, t50b307c3744373	FLOAT8) " +
//        "USING CSV WITH ('csvfile.null'='\\\\N', 'transient_lastDdlTime'='1402918396', 'csvfile.delimiter'='|')").close();
//    executeString("CREATE TABLE tm_d_sdp_dis_app_grp_cd (" +
//        "appindex_grp_cd TEXT, appindex_grp_nm TEXT, dis_app_grp_cd TEXT, dis_app_grp_nm TEXT, " +
//        "sort_seq TEXT, oper_dt_hms TEXT) ").close();
//
//    executeString("CREATE  TABLE tc_sdp_dp_seg_grp_cd (" +
//        "dp_seg_grp_cd TEXT, dp_seg_grp_nm TEXT, oper_dt_hms TEXT) ").close();
//    executeString("CREATE  TABLE tc_sdp_data_exhst_qty_cd (" +
//        "data_exhst_qty_cd TEXT, data_exhst_qty_nm TEXT, " +
//        "sort_seq TEXT, oper_dt_hms TEXT) ").close();
//
//    CatalogService catalog = testingCluster.getMaster().getCatalog();
//    String[] tableNames = new String[]{"vctaoz", "tm_d_sdp_dis_app_grp_cd", "tc_sdp_dp_seg_grp_cd", "tc_sdp_data_exhst_qty_cd"};
//    Path aaa = null;
//    for (String eachTable: tableNames) {
//      TableDesc tableDesc = catalog.getTableDesc(TajoConstants.DEFAULT_DATABASE_NAME, eachTable);
//
//      Path tablePath = tableDesc.getPath();
//      FileSystem fs = tablePath.getFileSystem(conf);
//      //fs.delete(tablePath, true);
//      Path localPath = new Path("file:///Users/seungunchoe/Downloads/Tajo_Patch/babokim/data/" + eachTable);
//      FileSystem localFs = FileSystem.getLocal(conf);
//      for (FileStatus eachFile: localFs.listStatus(localPath)) {
//        fs.copyFromLocalFile(eachFile.getPath(), tablePath);
//      }
//    }
//
//    ResultSet res = executeString(
//        "select count(*) from ("  +
//        "SELECT  dp_seg_grp_cd, T50b30503744372\n" +
//            "FROM vctaoz   \n" +
//            "INNER JOIN \"tm_d_sdp_dis_app_grp_cd\" \"tm_d_sdp_dis_app_grp_cd1\" \n" +
//            "    ON vctaoz.appindex_grp_cd = \"tm_d_sdp_dis_app_grp_cd1\".appindex_grp_cd \n" +
//            "INNER JOIN \"tc_sdp_dp_seg_grp_cd\" \"tc_sdp_dp_seg_grp_cd3\" \n" +
//            "    ON vctaoz.seg_grp_cd = \"tc_sdp_dp_seg_grp_cd3\".dp_seg_grp_cd \n" +
//            "INNER JOIN \"tc_sdp_data_exhst_qty_cd\" \"tc_sdp_data_exhst_qty_cd4\" \n" +
//            "    ON vctaoz.data_exhst_qty_cd1 = \"tc_sdp_data_exhst_qty_cd4\".data_exhst_qty_cd   \n" +
//            "ORDER BY T50b30503744372, appindex_grp_nm) a"
//    );
//
//    String expected = "?count\n" +
//        "-------------------------------\n" +
//        "329\n";
//
//    assertEquals(expected, resultSetToString(res));
//
//    res.close();
//  }
}
