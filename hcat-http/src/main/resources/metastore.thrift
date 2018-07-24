include "hcat.thrift"
namespace java com.hiido.hcat.thrift.protocol
namespace php Thrift.Php.HcatClient

struct FieldInfo {
    1: required string name,
    2: required string type,
    3: required bool isPart
}

service MetastoreService{
    list<FieldInfo> getColumns(1: string dbname, 2: string table) throws (1:hcat.AuthorizationException authException, 2: hcat.RuntimeException runTimeException, 3: hcat.NotFoundException notfoundException),
    string getPartitionPath(1: string dbname, 2: string table, 3: map<string, string> partitions) throws (1:hcat.AuthorizationException authException, 2: hcat.RuntimeException runTimeException, 3: hcat.NotFoundException notfoundException),
    void createDatabase(1: map<string, string> cipher, 2: string database) throws (1: hcat.AuthorizationException authException)
}