// This autogenerated skeleton file illustrates how to build a server.
// You should copy it to another filename to avoid overwriting it.

#include "TajoThriftService.h"
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TSimpleServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TBufferTransports.h>

using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;

using boost::shared_ptr;

using namespace  ::apache::tajo::thrift;

class TajoThriftServiceHandler : virtual public TajoThriftServiceIf {
 public:
  TajoThriftServiceHandler() {
    // Your initialization goes here
  }

  void submitQuery(TGetQueryStatusResponse& _return, const std::string& sessionId, const std::string& query, const bool isJson) {
    // Your implementation goes here
    printf("submitQuery\n");
  }

  void getQueryResult(TQueryResult& _return, const std::string& sessionId, const std::string& queryId, const int32_t fetchSize) {
    // Your implementation goes here
    printf("getQueryResult\n");
  }

  void getQueryStatus(TGetQueryStatusResponse& _return, const std::string& sessionId, const std::string& queryId) {
    // Your implementation goes here
    printf("getQueryStatus\n");
  }

  void closeQuery(TServerResponse& _return, const std::string& sessionId, const std::string& queryId) {
    // Your implementation goes here
    printf("closeQuery\n");
  }

  void updateQuery(TServerResponse& _return, const std::string& sessionId, const std::string& query) {
    // Your implementation goes here
    printf("updateQuery\n");
  }

  void createSession(TServerResponse& _return, const std::string& userId, const std::string& defaultDatabase) {
    // Your implementation goes here
    printf("createSession\n");
  }

  void closeSession(TServerResponse& _return, const std::string& sessionId) {
    // Your implementation goes here
    printf("closeSession\n");
  }

  void refreshSession(TServerResponse& _return, const std::string& sessionId) {
    // Your implementation goes here
    printf("refreshSession\n");
  }

  void selectDatabase(TServerResponse& _return, const std::string& sessionId, const std::string& database) {
    // Your implementation goes here
    printf("selectDatabase\n");
  }

  void getCurrentDatabase(std::string& _return, const std::string& sessionId) {
    // Your implementation goes here
    printf("getCurrentDatabase\n");
  }

  void killQuery(TServerResponse& _return, const std::string& sessionId, const std::string& queryId) {
    // Your implementation goes here
    printf("killQuery\n");
  }

  void getQueryList(std::vector<TBriefQueryInfo> & _return, const std::string& sessionId) {
    // Your implementation goes here
    printf("getQueryList\n");
  }

  bool existTable(const std::string& sessionId, const std::string& tableName) {
    // Your implementation goes here
    printf("existTable\n");
  }

  void getTableList(std::vector<std::string> & _return, const std::string& sessionId, const std::string& databaseName) {
    // Your implementation goes here
    printf("getTableList\n");
  }

  void getTableDesc(TTableDesc& _return, const std::string& sessionId, const std::string& tableName) {
    // Your implementation goes here
    printf("getTableDesc\n");
  }

  bool dropTable(const std::string& sessionId, const std::string& tableName, const bool purge) {
    // Your implementation goes here
    printf("dropTable\n");
  }

  void getAllDatabases(std::vector<std::string> & _return, const std::string& sessionId) {
    // Your implementation goes here
    printf("getAllDatabases\n");
  }

  bool createDatabase(const std::string& sessionId, const std::string& databaseName) {
    // Your implementation goes here
    printf("createDatabase\n");
  }

  bool dropDatabase(const std::string& sessionId, const std::string& databaseName) {
    // Your implementation goes here
    printf("dropDatabase\n");
  }

  bool existDatabase(const std::string& sessionId, const std::string& databaseName) {
    // Your implementation goes here
    printf("existDatabase\n");
  }

  void getAllSessionVariables(std::map<std::string, std::string> & _return, const std::string& sessionId) {
    // Your implementation goes here
    printf("getAllSessionVariables\n");
  }

  bool updateSessionVariable(const std::string& sessionId, const std::string& key, const std::string& value) {
    // Your implementation goes here
    printf("updateSessionVariable\n");
  }

  bool unsetSessionVariables(const std::string& sessionId, const std::string& key) {
    // Your implementation goes here
    printf("unsetSessionVariables\n");
  }

};

int main(int argc, char **argv) {
  int port = 9090;
  shared_ptr<TajoThriftServiceHandler> handler(new TajoThriftServiceHandler());
  shared_ptr<TProcessor> processor(new TajoThriftServiceProcessor(handler));
  shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
  shared_ptr<TTransportFactory> transportFactory(new TBufferedTransportFactory());
  shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());

  TSimpleServer server(processor, serverTransport, transportFactory, protocolFactory);
  server.serve();
  return 0;
}

