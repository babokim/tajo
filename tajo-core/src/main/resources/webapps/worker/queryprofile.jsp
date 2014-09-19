<%
  /*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements. See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership. The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
%>
<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>

<%@ page import="org.apache.tajo.QueryId" %>
<%@ page import="org.apache.tajo.master.querymaster.Query" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryMaster" %>
<%@ page import="org.apache.tajo.master.querymaster.QueryMasterTask" %>
<%@ page import="org.apache.tajo.master.querymaster.SubQuery" %>
<%@ page import="org.apache.tajo.util.JSPUtil" %>
<%@ page import="org.apache.tajo.util.TajoIdUtils" %>
<%@ page import="org.apache.tajo.webapp.StaticHttpServer" %>
<%@ page import="org.apache.tajo.worker.TajoWorker" %>
<%@ page import="org.apache.tajo.ipc.TajoWorkerProtocol.QueryProfileDataListProto" %>
<%@ page import="org.apache.tajo.ipc.TajoWorkerProtocol.QueryProfileDataListProto.EBQueryProfileData" %>
<%@ page import="java.text.SimpleDateFormat" %>
<%@ page import="java.util.List" %>

<%
  QueryId queryId = TajoIdUtils.parseQueryId(request.getParameter("queryId"));

  TajoWorker tajoWorker = (TajoWorker) StaticHttpServer.getInstance().getAttribute("tajo.info.server.object");
  QueryMaster queryMaster = tajoWorker.getWorkerContext().getQueryMasterManagerService().getQueryMaster();
  List<QueryProfileDataListProto> profileData = queryMaster.getQueryProfileData(queryId);

  if (profileData == null || profileData.isEmpty()) {
    out.write("<script type='text/javascript'>alert('no query profile data'); history.back(0); </script>");
    return;
  }
%>

<!DOCTYPE html PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN" "http://www.w3.org/TR/html4/loose.dtd">
<html>
<head>
  <link rel="stylesheet" type="text/css" href="/static/style.css"/>
  <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
  <title>Query Detail Info</title>
</head>
<body>
<%@ include file="header.jsp"%>
<div class='contents'>
  <h2>Tajo Worker: <a href='index.jsp'><%=tajoWorker.getWorkerContext().getWorkerName()%></a></h2>
  <hr/>
  <h3><%=queryId.toString()%> <a href='querydetail.jsp?queryId=<%=queryId%>'>[Query Detail]</a><a href='profilejson.jsp?queryId=<%=queryId%>'>[JSON]</a></h3>
  <pre>
  <%=JSPUtil.profileToString(profileData)%>
  </pre>
</div>
</body>
</html>
